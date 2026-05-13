FROM mcr.microsoft.com/playwright/python:v1.52.0-jammy

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends wget gnupg ca-certificates gzip \
    && echo "deb http://apt.postgresql.org/pub/repos/apt jammy-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && wget -qO - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client-18 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "collector.py"]
