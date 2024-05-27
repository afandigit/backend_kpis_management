FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    libmariadb-dev-compat \
    pkg-config \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /backend

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5099

CMD ["python", "app.py"]


