FROM mcr.microsoft.com/playwright/python:v1.52.0-jammy

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client gzip \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "collector.py"]
