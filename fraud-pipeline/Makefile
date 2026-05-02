.PHONY: install run-producer test-topics logs

install:
	pip install -r data_generator/requirements.txt

run-producer:
	cd data_generator && python producer.py

test-topics:
	docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

logs:
	docker compose logs -f