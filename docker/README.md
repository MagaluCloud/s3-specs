# Running S3 Specs with Docker

1. Edit params.example.yaml file adding at least 3 profiles with a valid credentials.

2. Pull the container
```bash
docker build -t s3specs -f docker/Dockerfile . 
```

3. Run all tests
```bash
docker run --rm -it s3specs uv run pytest --config ./params.example.yaml ./docs/ --tb=line
```

4. Run tests by category
```bash
docker run --rm -it s3specs uv run pytest --config ./params.example.yaml ./docs/ -m "categoryname" --tb=line
```

5. Run tests excluding category
```bash
docker run --rm -it s3specs uv run pytest --config ./params.example.yaml ./docs/ -m "not categoryname" --tb=line
```