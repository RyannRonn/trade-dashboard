FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server/ ./server/
COPY trade.db .
COPY trade.html .
COPY trade_data_v2.json .
COPY provisional.html .
COPY provisional_data.json .
COPY business_days.json .

EXPOSE 8000

CMD ["sh", "-c", "uvicorn server.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
