import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "trade.db")
JSON_PATH = os.path.join(BASE_DIR, "trade_data_v2.json")
HTML_PATH = os.path.join(BASE_DIR, "trade.html")
