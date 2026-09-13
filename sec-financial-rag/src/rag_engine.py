import os
import argparse
import requests
from typing import List, Dict, Any, Optional
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.http import models

# --- CONFIGURATION ---
QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", 6333))
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
COLLECTION_NAME = "financial_sec_chunks"
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
VECTOR_SIZE = 384

class RAGQueryEngine:
    def __init__(self):
        print("🔍 Initializing RAG Query Engine...")
        self.embedding_model = SentenceTransformer(MODEL_NAME)
        self.qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
        self._ensure_collection_exists()

    def _ensure_collection_exists(self):
        """Verifies collection presence in Qdrant; creates it if missing."""
        collections = self.qdrant_client.get_collections().collections
        exists = any(c.name == COLLECTION_NAME for c in collections)
        if not exists:
            print(f"⚠️ Collection '{COLLECTION_NAME}' not found in Qdrant. Creating new collection...")
            self.qdrant_client.create_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=models.VectorParams(
                    size=VECTOR_SIZE,
                    distance=models.Distance.COSINE
                )
            )

    def retrieve_context(
        self, 
        query: str, 
        ticker: Optional[str] = None, 
        year: Optional[str] = None, 
        top_k: int = 4
    ) -> List[Dict[str, Any]]:
        """Generates query embeddings and searches Qdrant using standard search method."""
        query_vector = self.embedding_model.encode(query).tolist()

        must_conditions = []
        if ticker:
            must_conditions.append(
                models.FieldCondition(
                    key="ticker",
                    match=models.MatchValue(value=ticker.upper())
                )
            )
        if year:
            must_conditions.append(
                models.FieldCondition(
                    key="year",
                    match=models.MatchValue(value=str(year))
                )
            )

        query_filter = models.Filter(must=must_conditions) if must_conditions else None

        search_results = self.qdrant_client.search(
            collection_name=COLLECTION_NAME,
            query_vector=query_vector,
            query_filter=query_filter,
            limit=top_k
        )

        retrieved_chunks = []
        for point in search_results:
            retrieved_chunks.append({
                "score": round(point.score, 4),
                "ticker": point.payload.get("ticker") if point.payload else None,
                "year": point.payload.get("year") if point.payload else None,
                "form_type": point.payload.get("form_type") if point.payload else None,
                "text": point.payload.get("text") if point.payload else ""
            })

        return retrieved_chunks

    def generate_answer_prompt(self, query: str, context_chunks: List[Dict[str, Any]]) -> str:
        """Synthesizes an LLM context prompt using retrieved document chunks."""
        if not context_chunks:
            return "No relevant SEC filing documents found matching your query filters."

        context_blocks = []
        for c in context_chunks:
            source_tag = f"[Source: Ticker={c['ticker']}, Year={c['year']}, Form={c['form_type']} | Score={c['score']}]"
            context_blocks.append(f"{source_tag}\n{c['text']}")

        context_str = "\n\n---\n\n".join(context_blocks)

        prompt = f"""You are an expert financial analyst. Answer the question accurately using ONLY the provided SEC filing context below.
If the information cannot be determined from the context, state clearly that you do not have sufficient information.

CONTEXT:
{context_str}

QUESTION:
{query}

ANSWER:"""
        return prompt

    def query_llm(self, prompt: str, llm_model: str = "llama3.2:1b") -> str:
        """Sends the assembled prompt to local Ollama instance with streaming output."""
        print(f"\n🤖 Querying Ollama model ('{llm_model}')...\n")
        try:
            response = requests.post(
                f"{OLLAMA_HOST}/api/generate",
                json={
                    "model": llm_model,
                    "prompt": prompt,
                    "stream": True
                },
                stream=True,
                timeout=300
            )
            
            if response.status_code != 200:
                return f"⚠️ Ollama Error ({response.status_code}): {response.text}"

            full_response = []
            for line in response.iter_lines():
                if line:
                    import json
                    data = json.loads(line.decode("utf-8"))
                    token = data.get("response", "")
                    print(token, end="", flush=True)
                    full_response.append(token)
            
            print()  # New line after completion
            return "".join(full_response)

        except Exception as e:
            return f"⚠️ Could not connect to Ollama at {OLLAMA_HOST}. Error: {e}"
def main():
    parser = argparse.ArgumentParser(description="SEC Financial RAG Query CLI")
    parser.add_argument("--query", type=str, required=True, help="Question to ask regarding SEC filings")
    parser.add_argument("--ticker", type=str, default=None, help="Filter by ticker symbol (e.g. AAPL, NVDA)")
    parser.add_argument("--year", type=str, default=None, help="Filter by filing year (e.g. 2024)")
    parser.add_argument("--top_k", type=int, default=3, help="Number of context chunks to retrieve")
    parser.add_argument("--llm_model", type=str, default="llama3.2:1b", help="Ollama LLM model name")
    args = parser.parse_args()

    engine = RAGQueryEngine()
    print(f"\n🔎 Querying: '{args.query}' (Ticker Filter: {args.ticker}, Year Filter: {args.year})")
    
    results = engine.retrieve_context(
        query=args.query,
        ticker=args.ticker,
        year=args.year,
        top_k=args.top_k
    )

    print(f"\n📥 Retrieved {len(results)} Relevant Context Chunks:")
    for idx, chunk in enumerate(results, 1):
        print(f"\n[{idx}] {chunk['ticker']} ({chunk['year']}) - Similarity Score: {chunk['score']}")
        print(f"Excerpt: {chunk['text'][:250]}...")

    prompt = engine.generate_answer_prompt(args.query, results)
    llm_response = engine.query_llm(prompt, llm_model=args.llm_model)

    print("\n" + "="*80)
    print(f"🤖 GENERATED LLM RESPONSE ({args.llm_model})")
    print("="*80)
    print(llm_response)

if __name__ == "__main__":
    main()
