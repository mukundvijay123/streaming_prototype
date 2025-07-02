FROM python:3.11-slim as base

WORKDIR /app

COPY src/* .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "mcpServer.py"]