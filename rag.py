from langchain_huggingface import HuggingFaceEmbeddings
from langchain_community.vectorstores import Qdrant
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from PyPDF2 import PdfReader
import hashlib
from openai import OpenAI
import logging
import os
import time
from qdrant_client import QdrantClient
from qdrant_client.http import models

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Конфигурация Qdrant ---
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_COLLECTION_NAME = os.getenv("QDRANT_COLLECTION_NAME", "nutri-bot")

if not QDRANT_URL or not QDRANT_API_KEY:
    logger.error("❗ Не заданы QDRANT_URL или QDRANT_API_KEY")
    raise ValueError("Qdrant: URL и API-ключ обязательны")

# --- Конфигурация OpenRouter (Qwen) ---
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
if not OPENROUTER_API_KEY:
    logger.error("❗ Не задан OPENROUTER_API_KEY")
    raise ValueError("OpenRouter: API-ключ обязателен")

# Настройка OpenAI-совместимого клиента для OpenRouter
from openai import OpenAI
openrouter_client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
)

# --- Эмбеддинги ---
embeddings = HuggingFaceEmbeddings(model_name=os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"))

# --- Клиент Qdrant ---
client = QdrantClient(
    url=QDRANT_URL,
    api_key=QDRANT_API_KEY,
    timeout=60  # Увеличиваем таймаут для Railway
)

# --- Проверка/создание коллекции ---
try:
    client.get_collection(QDRANT_COLLECTION_NAME)
    logger.info(f"✅ Коллекция '{QDRANT_COLLECTION_NAME}' уже существует")
except Exception as e:
    logger.info(f"🛠️ Создаю коллекцию '{QDRANT_COLLECTION_NAME}'...")
    client.create_collection(
        collection_name=QDRANT_COLLECTION_NAME,
        vectors_config=models.VectorParams(
            size=384,
            distance=models.Distance.COSINE
        )
    )
    logger.info(f"✅ Коллекция '{QDRANT_COLLECTION_NAME}' создана")

# --- Создание индекса по полю "source" ---
try:
    info = client.get_collection(QDRANT_COLLECTION_NAME)
    payload_schema = info.payload_schema
    if "source" not in payload_schema:
        logger.info("🛠️ Создаю индекс по полю 'source'...")
        client.create_payload_index(
            collection_name=QDRANT_COLLECTION_NAME,
            field_name="source",
            field_schema=models.PayloadSchemaType.KEYWORD
        )
        logger.info("✅ Индекс по полю 'source' создан")
    else:
        logger.info("✅ Индекс по полю 'source' уже существует")
except Exception as e:
    logger.warning(f"⚠️ Ошибка при создании индекса: {e}")

# --- Векторное хранилище ---
vectorstore = Qdrant(
    client=client,
    collection_name=QDRANT_COLLECTION_NAME,
    embeddings=embeddings
)


# --- Функция проверки доступности сервисов ---
def check_services_health():
    """Проверяет доступность Qdrant и OpenRouter при запуске"""
    logger.info("🔧 Проверяю доступность сервисов...")
    
    # Проверка Qdrant
    try:
        logger.info("🔗 Проверяю подключение к Qdrant...")
        collection_info = client.get_collection(QDRANT_COLLECTION_NAME)
        logger.info(f"✅ Qdrant OK - Коллекция: {QDRANT_COLLECTION_NAME}, Векторов: {collection_info.points_count}")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к Qdrant: {e}")
        logger.error("🔧 Проверьте переменные QDRANT_URL и QDRANT_API_KEY")
        return False
    
    # Проверка OpenRouter
    try:
        logger.info("🤖 Проверяю подключение к OpenRouter...")
        test_response = openrouter_client.chat.completions.create(
            model="qwen/qwen-2.5-coder-32b-instruct",
            messages=[{"role": "user", "content": "Test"}],
            max_tokens=5,
            timeout=10
        )
        logger.info("✅ OpenRouter OK - API доступен")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к OpenRouter: {e}")
        logger.error("🔧 Проверьте переменную OPENROUTER_API_KEY")
        return False
    
    logger.info("🎉 Все сервисы доступны!")
    return True


# --- Функция: вызов Qwen через OpenRouter ---
def call_qwen(prompt: str) -> str:
    logger.info("🤖 Отправляю запрос к Qwen через OpenRouter...")
    try:
        chat_completion = openrouter_client.chat.completions.create(
            model="qwen/qwen-2.5-coder-32b-instruct",
            messages=[
                {
                    "role": "system",
                    "content": """Ты — ассистент-помощник, Ксения в образовательной Telegram-группе по нутрициологии. Отвечай на основе информации из предоставленных учебных материалов. Ты всегда с радостью готова помочь студентам.

КРИТИЧЕСКИ ВАЖНО:
1. ВСЕГДА анализируй ВСЮ предоставленную информацию из контекста
2. Если есть ЛЮБАЯ релевантная информация (даже косвенная) — формируй полезный ответ
3. НИКОГДА не говори "нет информации", если в контексте есть хоть что-то по теме
4. Даже если информация неполная — используй её для базового ответа
5. Не используй плохую фразу "По имеющимся данным в предоставленном контексте"

ПРАВИЛА ОТВЕТА:
1. МАКСИМАЛЬНО используй всю доступную информацию из контекста
2. При неполной информации: "В моих данных..." + то что знаешь
3. Только при ПОЛНОМ отсутствии ЛЮБОЙ релевантной информации скажи: "затрудняюсь ответить"
4. При медицинских симптомах добавляй: "Обратитесь к своему куратору"
5. ПИШИ НА РУССКОМ, будь кратким и практичным
6. Если проблема требует помощи специалиста, то добавляй: "Обратитесь к своему куратору"
7. ПРИОРИТЕТ: Дать максимально полезный ответ на основе имеющихся материалов"""
                },
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            temperature=0.1,  # Низкая температура для точности
            max_tokens=1000,
            timeout=30  # Таймаут для OpenRouter запросов
        )
        logger.info(f"✅ Получен ответ от OpenRouter: {len(chat_completion.choices[0].message.content)} символов")
        return chat_completion.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"❌ Ошибка при вызове Qwen через OpenRouter: {e}", exc_info=True)
        return "Извините, сейчас не могу сформировать ответ."


# --- Формирование RAG-промпта ---
def make_rag_prompt(context: str, question: str) -> str:
    return f"""
КОНТЕКСТ ИЗ УЧЕБНЫХ МАТЕРИАЛОВ:
{context}

ВОПРОС СТУДЕНТА: {question}

ИНСТРУКЦИЯ: ОБЯЗАТЕЛЬНО анализируй ВСЮ предоставленную информацию из контекста. Если есть ЛЮБАЯ релевантная информация (даже частично подходящая) — используй её для полезного ответа. НЕ говори "нет информации", если в контексте есть что-то по теме. Помоги студенту получить максимум пользы из имеющихся материалов.
""".strip()


# --- Основная функция получения ответа ---
def get_answer(question: str) -> str:
    logger.info(f"🔍 Начинаю обработку вопроса: '{question[:100]}...'")
    
    try:
        # Проверяем подключение к Qdrant
        logger.info("🔗 Проверяю подключение к Qdrant...")
        try:
            collection_info = client.get_collection(QDRANT_COLLECTION_NAME)
            logger.info(f"✅ Подключение к Qdrant OK. Коллекция: {QDRANT_COLLECTION_NAME}")
            logger.info(f"📊 Векторов в коллекции: {collection_info.points_count}")
        except Exception as qdrant_error:
            logger.error(f"❌ Ошибка подключения к Qdrant: {qdrant_error}")
            raise ValueError(f"Qdrant connection failed: {qdrant_error}")

        # Улучшенный поиск с MMR для более разнообразных и релевантных результатов
        logger.info("🔍 Выполняю поиск по векторной базе...")
        retriever = vectorstore.as_retriever(
            search_type="mmr",  # Maximum Marginal Relevance - снижает дублирование
            search_kwargs={
                "k": 7,           # Увеличиваем количество результатов
                "fetch_k": 15,    # Больше кандидатов для выбора
                "lambda_mult": 0.7 # Баланс между релевантностью (1.0) и разнообразием (0.0)
            }
        )
        
        try:
            # Добавляем обертку с таймаутом для поиска
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            docs = retriever.invoke(question)
            logger.info(f"✅ Поиск завершен. Найдено документов: {len(docs)}")
        except Exception as search_error:
            logger.error(f"❌ Ошибка при поиске в векторной базе: {search_error}")
            # Более детальная диагностика ошибки
            if "timeout" in str(search_error).lower():
                logger.error("⏰ Превышено время ожидания поиска")
            elif "connection" in str(search_error).lower():
                logger.error("🔗 Проблема с подключением к Qdrant")
            raise ValueError(f"Vector search failed: {search_error}")
        
        # Отладочная информация
        logger.info(f"🔍 Найдено документов для вопроса '{question[:50]}...': {len(docs)}")
        for i, doc in enumerate(docs):
            source = doc.metadata.get('source', 'Неизвестно')
            page = doc.metadata.get('page', 'Неизвестно')  
            content_preview = doc.page_content[:100] + "..." if len(doc.page_content) > 100 else doc.page_content
            logger.info(f"   {i+1}. {source} (стр.{page}): {content_preview}")

        if not docs or not any(doc.page_content.strip() for doc in docs):
            logger.warning("⚠️ Не найдено релевантных документов")
            raise ValueError("No context retrieved")

        context = "\n\n".join([doc.page_content for doc in docs if doc.page_content.strip()])
        logger.info(f"📄 Контекст сформирован, длина: {len(context)} символов")
        
        prompt = make_rag_prompt(context, question)
        logger.info("🤖 Отправляю запрос к Qwen...")
        
        try:
            answer = call_qwen(prompt)
            logger.info(f"✅ Получен ответ от Qwen, длина: {len(answer)} символов")
        except Exception as llm_error:
            logger.error(f"❌ Ошибка при обращении к Qwen: {llm_error}")
            raise ValueError(f"LLM call failed: {llm_error}")
        
        # Логируем ответ LLM для отладки
        logger.info(f"📝 Ответ LLM: {answer[:200]}...")
        
        return answer
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА в get_answer: {e}", exc_info=True)
        # Возвращаем фразу, которую main.py распознает как отсутствие контекста
        return "затрудняюсь ответить"


# --- Функции для хеширования PDF ---
def calculate_pdf_hash(pdf_path: str) -> str:
    """Вычисляет SHA-256 хеш PDF файла"""
    try:
        with open(pdf_path, 'rb') as f:
            pdf_content = f.read()
            return hashlib.sha256(pdf_content).hexdigest()
    except Exception as e:
        logger.error(f"Ошибка при вычислении хеша PDF: {e}")
        return ""

def get_stored_hash(filename: str) -> str:
    """Получает сохраненный хеш файла из Qdrant"""
    try:
        search_result = client.scroll(
            collection_name=QDRANT_COLLECTION_NAME,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="source",
                        match=models.MatchValue(value=filename)
                    )
                ]
            ),
            limit=1,
            with_payload=True
        )
        points = search_result[0]
        if points and points[0].payload:
            return points[0].payload.get('file_hash', '')
        return ""
    except Exception as e:
        logger.debug(f"Не удалось получить хеш для {filename}: {e}")
        return ""

def extract_text_with_metadata(pdf_path: str, filename: str) -> list:
    """Извлекает текст из PDF с метаданными страниц и заголовков"""
    try:
        reader = PdfReader(pdf_path)
        documents = []
        
        for page_num, page in enumerate(reader.pages, 1):
            extracted = page.extract_text()
            if extracted and extracted.strip():
                # Пытаемся найти заголовки (простая эвристика)
                lines = extracted.split('\n')
                potential_title = ""
                
                # Ищем первую непустую строку как потенциальный заголовок
                for line in lines[:5]:  # Смотрим только первые 5 строк
                    clean_line = line.strip()
                    if clean_line and len(clean_line) < 100:  # Заголовки обычно короче
                        potential_title = clean_line
                        break
                
                documents.append({
                    'text': extracted,
                    'metadata': {
                        'source': filename,
                        'page': page_num,
                        'title': potential_title if potential_title else f"Страница {page_num}",
                        'total_pages': len(reader.pages)
                    }
                })
        
        logger.info(f"✅ Извлечен текст из {len(documents)} страниц")
        return documents
        
    except Exception as e:
        logger.error(f"Ошибка извлечения текста с метаданными: {e}")
        return []

# --- Обновление базы знаний ---
def update_knowledge_base(pdf_path: str, filename: str):
    try:
        logger.info(f"🔄 Начало обработки PDF: {filename}")

        # Вычисляем хеш нового файла
        new_hash = calculate_pdf_hash(pdf_path)
        if not new_hash:
            raise ValueError("Не удалось вычислить хеш PDF файла")
        
        logger.info(f"🔐 Хеш нового файла: {new_hash[:16]}...")

        # Проверяем существующий хеш
        stored_hash = get_stored_hash(filename)
        if stored_hash:
            logger.info(f"🔐 Хеш в базе: {stored_hash[:16]}...")
            
            if new_hash == stored_hash:
                logger.info("✅ Файл не изменился, пропускаем обновление")
                return  # Файл не изменился, ничего не делаем
            else:
                logger.info("🔄 Файл изменился, обновляем...")
        else:
            logger.info("📝 Новый файл, добавляем в базу...")

        # Удаляем старую версию если есть
        if stored_hash:  # Есть старая версия
            logger.info(f"🗑️ Удаление старой версии {filename} из Qdrant...")
            max_retries = 3
            deletion_successful = False
            
            for attempt in range(max_retries):
                try:
                    search_result = client.scroll(
                        collection_name=QDRANT_COLLECTION_NAME,
                        scroll_filter=models.Filter(
                            must=[
                                models.FieldCondition(
                                    key="source",
                                    match=models.MatchValue(value=filename)
                                )
                            ]
                        ),
                        limit=1000
                    )
                    points = search_result[0]
                    if points:
                        point_ids = [point.id for point in points]
                        client.delete(
                            collection_name=QDRANT_COLLECTION_NAME,
                            points_selector=models.PointIdsList(points=point_ids)
                        )
                        logger.info(f"✅ Удалено {len(point_ids)} векторов для {filename}")
                    deletion_successful = True
                    break
                except Exception as e:
                    logger.warning(f"⚠️ Попытка {attempt + 1}/{max_retries} удаления неудачна: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(1)  # Ждем секунду перед повторной попыткой
                    
            if not deletion_successful:
                error_msg = f"❌ Критическая ошибка: не удалось удалить старую версию {filename} после {max_retries} попыток."
                logger.error(error_msg)
                raise RuntimeError(error_msg)

        # Извлекаем текст с метаданными
        logger.info("📄 Извлечение текста с метаданными...")
        page_documents = extract_text_with_metadata(pdf_path, filename)
        if not page_documents:
            raise ValueError("PDF не содержит текста или не удалось его извлечь")

        # Улучшенное разбиение на чанки
        logger.info("✂️ Семантическое разбиение на чанки...")
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=512,  # Увеличенный размер чанка
            chunk_overlap=100,  # Больше перекрытие для контекста
            length_function=len,
            separators=["\n\n", "\n", ". ", "! ", "? ", ", ", " ", ""]
        )
        
        all_docs = []
        for page_doc in page_documents:
            chunks = splitter.split_text(page_doc['text'])
            
            for i, chunk in enumerate(chunks):
                if chunk.strip():  # Только непустые чанки
                    # Расширенные метаданные
                    metadata = {
                        **page_doc['metadata'],  # Берем метаданные страницы
                        'file_hash': new_hash,   # Добавляем хеш файла
                        'chunk_id': i,           # Номер чанка на странице
                        'chunk_size': len(chunk) # Размер чанка
                    }
                    
                    all_docs.append(Document(
                        page_content=chunk,
                        metadata=metadata
                    ))

        logger.info(f"✅ Создано {len(all_docs)} чанков с метаданными")

        # Добавляем в Qdrant
        logger.info("📤 Добавляю векторы в Qdrant...")
        vectorstore.add_documents(all_docs)
        logger.info(f"✅ Успешно добавлено {len(all_docs)} чанков в Qdrant")
        logger.info(f"🎯 Файл {filename} (хеш: {new_hash[:16]}...) успешно обработан")

    except Exception as e:
        logger.error(f"❌ Ошибка при обработке PDF: {e}", exc_info=True)
        raise