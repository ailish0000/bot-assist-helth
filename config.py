import os
from dotenv import load_dotenv
load_dotenv()

# Токены
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# Временная папка
TEMP_DIR = "temp_pdfs"
os.makedirs(TEMP_DIR, exist_ok=True)

# Модели
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
QWEN_MODEL = "qwen-max"

# (Опционально) ID для загрузки PDF, если хотите ограничить
ADMIN_UPLOAD_IDS = list(map(int, os.getenv("ADMIN_UPLOAD_IDS", "").split(","))) if os.getenv("ADMIN_UPLOAD_IDS") else []
