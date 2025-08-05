#!/usr/bin/env python3
"""
Диагностический скрипт для проверки здоровья всех сервисов бота
"""
import os
import sys
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def check_environment_variables():
    """Проверяет наличие всех необходимых переменных окружения"""
    logger.info("🔧 Проверка переменных окружения...")
    
    required_vars = [
        'TELEGRAM_BOT_TOKEN',
        'OPENROUTER_API_KEY', 
        'QDRANT_URL',
        'QDRANT_API_KEY'
    ]
    
    optional_vars = [
        'QDRANT_COLLECTION_NAME',
        'ADMIN_UPLOAD_IDS',
        'EMBEDDING_MODEL'
    ]
    
    missing_vars = []
    present_vars = []
    
    for var in required_vars:
        if os.getenv(var):
            # Маскируем значение для безопасности
            value = os.getenv(var)
            masked = value[:8] + "..." + value[-4:] if len(value) > 12 else "***"
            present_vars.append(f"{var}: {masked}")
        else:
            missing_vars.append(var)
    
    for var in optional_vars:
        if os.getenv(var):
            value = os.getenv(var)
            # Для некритичных переменных показываем полное значение
            if var in ['QDRANT_COLLECTION_NAME', 'EMBEDDING_MODEL']:
                present_vars.append(f"{var}: {value}")
            else:
                masked = value[:8] + "..." if len(value) > 8 else value
                present_vars.append(f"{var}: {masked}")
    
    logger.info("✅ Найденные переменные:")
    for var in present_vars:
        logger.info(f"   {var}")
    
    if missing_vars:
        logger.error("❌ Отсутствующие обязательные переменные:")
        for var in missing_vars:
            logger.error(f"   {var}")
        return False
    
    logger.info("✅ Все обязательные переменные окружения присутствуют")
    return True

def check_telegram_bot():
    """Проверяет подключение к Telegram Bot API"""
    logger.info("🤖 Проверка Telegram Bot...")
    
    try:
        from aiogram import Bot
        from config import TELEGRAM_BOT_TOKEN
        
        if not TELEGRAM_BOT_TOKEN:
            logger.error("❌ TELEGRAM_BOT_TOKEN не задан")
            return False
        
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        
        # Простая синхронная проверка
        import asyncio
        async def test_bot():
            try:
                me = await bot.get_me()
                logger.info(f"✅ Telegram Bot OK: @{me.username} ({me.first_name})")
                await bot.session.close()
                return True
            except Exception as e:
                logger.error(f"❌ Ошибка Telegram Bot: {e}")
                return False
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(test_bot())
        loop.close()
        return result
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при проверке Telegram: {e}")
        return False

def check_qdrant():
    """Проверяет подключение к Qdrant"""
    logger.info("🗄️ Проверка Qdrant...")
    
    try:
        from qdrant_client import QdrantClient
        
        QDRANT_URL = os.getenv("QDRANT_URL")
        QDRANT_API_KEY = os.getenv("QDRANT_API_KEY") 
        QDRANT_COLLECTION_NAME = os.getenv("QDRANT_COLLECTION_NAME", "nutri-bot")
        
        if not QDRANT_URL or not QDRANT_API_KEY:
            logger.error("❌ QDRANT_URL или QDRANT_API_KEY не заданы")
            return False
        
        client = QdrantClient(
            url=QDRANT_URL,
            api_key=QDRANT_API_KEY,
            timeout=10
        )
        
        # Проверяем подключение
        collections = client.get_collections()
        logger.info(f"✅ Qdrant подключение OK. Коллекций: {len(collections.collections)}")
        
        # Проверяем нашу коллекцию
        try:
            collection_info = client.get_collection(QDRANT_COLLECTION_NAME)
            logger.info(f"✅ Коллекция '{QDRANT_COLLECTION_NAME}' найдена. Векторов: {collection_info.points_count}")
        except Exception as e:
            logger.warning(f"⚠️ Коллекция '{QDRANT_COLLECTION_NAME}' не найдена: {e}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к Qdrant: {e}")
        return False

def check_openrouter():
    """Проверяет подключение к OpenRouter"""
    logger.info("🧠 Проверка OpenRouter...")
    
    try:
        from openai import OpenAI
        
        OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
        if not OPENROUTER_API_KEY:
            logger.error("❌ OPENROUTER_API_KEY не задан")
            return False
        
        client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=OPENROUTER_API_KEY,
        )
        
        # Делаем тестовый запрос
        response = client.chat.completions.create(
            model="qwen/qwen-2.5-coder-32b-instruct",
            messages=[{"role": "user", "content": "Скажи 'тест'"}],
            max_tokens=5,
            timeout=10
        )
        
        logger.info("✅ OpenRouter API OK")
        logger.info(f"✅ Тестовый ответ: {response.choices[0].message.content}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к OpenRouter: {e}")
        return False

def check_embeddings():
    """Проверяет загрузку модели эмбеддингов"""
    logger.info("🔤 Проверка модели эмбеддингов...")
    
    try:
        from langchain_huggingface import HuggingFaceEmbeddings
        
        model_name = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
        logger.info(f"📥 Загружаю модель: {model_name}")
        
        embeddings = HuggingFaceEmbeddings(model_name=model_name)
        
        # Тестируем эмбеддинг
        test_text = "Тестовый текст для проверки эмбеддингов"
        test_embedding = embeddings.embed_query(test_text)
        
        logger.info(f"✅ Модель эмбеддингов OK. Размерность: {len(test_embedding)}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки модели эмбеддингов: {e}")
        return False

def main():
    """Основная функция диагностики"""
    logger.info("🚀 Начало диагностики...")
    logger.info(f"🕐 Время: {datetime.now().isoformat()}")
    logger.info(f"🐍 Python: {sys.version}")
    logger.info("=" * 60)
    
    checks = [
        ("Переменные окружения", check_environment_variables),
        ("Telegram Bot", check_telegram_bot),
        ("Qdrant", check_qdrant),
        ("OpenRouter", check_openrouter),
        ("Эмбеддинги", check_embeddings)
    ]
    
    results = {}
    for name, check_func in checks:
        logger.info(f"\n🔍 {name}...")
        try:
            results[name] = check_func()
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в {name}: {e}")
            results[name] = False
    
    logger.info("\n" + "=" * 60)
    logger.info("📊 ИТОГОВЫЙ ОТЧЕТ:")
    
    all_ok = True
    for name, status in results.items():
        icon = "✅" if status else "❌"
        logger.info(f"{icon} {name}: {'OK' if status else 'FAILED'}")
        if not status:
            all_ok = False
    
    if all_ok:
        logger.info("\n🎉 ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ! Бот готов к работе.")
        return 0
    else:
        logger.error("\n💥 ЕСТЬ ПРОБЛЕМЫ! Исправьте ошибки перед запуском бота.")
        return 1

if __name__ == "__main__":
    sys.exit(main())