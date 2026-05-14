FROM mcr.microsoft.com/playwright/python:v1.52.0-jammy

WORKDIR /app

ARG PG_MAJOR=18

RUN apt-get update \
    && apt-get install -y --no-install-recommends wget gnupg ca-certificates gzip \
    && install -d -m 0755 /etc/apt/keyrings \
    && wget -qO - https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        | gpg --dearmor -o /etc/apt/keyrings/postgresql.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt jammy-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client-${PG_MAJOR} \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "app.py"]
