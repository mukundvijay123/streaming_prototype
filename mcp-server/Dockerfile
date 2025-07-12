FROM python:3.11-slim as base

WORKDIR /app

COPY src/* .

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ curl \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "mcpServer.py"]