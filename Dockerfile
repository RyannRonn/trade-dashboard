FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server/ ./server/
COPY collector/ ./collector/
COPY static/ ./static/
COPY trade.html .
COPY trade_data_v2.json .
COPY provisional.html .
COPY provisional_data.json .
COPY business_days.json .
COPY confirmed_companies.json .

# trade.db는 git에 두지 않고 빌드 시 trade_data_v2.json에서 생성 (149MB > GitHub 100MB 한도 회피)
RUN python -m collector.migrate_json

EXPOSE 8000

CMD ["sh", "-c", "uvicorn server.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
