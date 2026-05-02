SELECT *
FROM read_parquet(
    '../../delta/silver/**/*.parquet'
)