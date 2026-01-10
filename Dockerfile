FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY bromestriker /app/bromestriker
COPY README.md /app/README.md

# data volume
RUN mkdir -p /app/data

CMD ["python", "-m", "bromestriker"]
