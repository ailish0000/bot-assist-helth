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
from data_cleaner import DataCleaner

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Пытаемся загрузить полную версию с ML моделями
    from nlp_processor import NLPProcessor
    logger.info("✅ Загружена полная версия NLP процессора")
except ImportError as e:
    # Если не удается, используем облегченную версию
    logger.warning(f"⚠️ Полная версия NLP недоступна: {e}")
    logger.info("🔄 Загружаю облегченную версию NLP процессора...")
    from nlp_processor_lite import NLPProcessor

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

# --- NLP Процессор ---
nlp_processor = NLPProcessor()
logger.info("🧠 NLP Процессор инициализирован")

# --- Очиститель данных ---
data_cleaner = DataCleaner()
logger.info("🧹 Модуль очистки данных инициализирован")

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
                    "content": """Ты — ассистент-помощник Ксения в образовательной Telegram-группе по нутрициологии. Отвечай на основе информации из предоставленных учебных материалов. Ты всегда с радостью готова помочь студентам.

КРИТИЧЕСКИ ВАЖНО - АЛГОРИТМ ОТВЕТА:
1. Найди ЛЮБУЮ информацию, которая хотя бы частично связана с вопросом
2. Если нашел хоть что-то релевантное - ОБЯЗАТЕЛЬНО используй это для ответа
3. НЕ отказывайся отвечать, если есть хотя бы косвенная информация
4. Ищи синонимы, похожие темы.

СТРАТЕГИЯ ПОИСКА ИНФОРМАЦИИ:
- Рецепты → ищи кулинарные советы, ингредиенты, способы приготовления
- Питание → ищи информацию о продуктах, диетах, здоровье
- Здоровье → ищи симптомы, рекомендации, методы лечения  
- Продукты → ищи состав, свойства, применение
- Организационные вопросы → ищи информацию об обучении, кураторах, сроках, процедурах, правилах курса
- Обучение → ищи расписание, длительность, этапы, требования, процесс
- Кураторы → ищи контакты, время работы, обязанности, способы связи
- Административные вопросы → ищи правила, регламенты, процедуры, документооборот

ПРАВИЛА ФОРМУЛИРОВКИ ОТВЕТА:
1. НИКОГДА не говори "нет информации" или "затрудняюсь ответить", если в контексте есть что-то по теме
2. Начинай сразу выдавать полезный ответ
3. Вместо фразы "По имеющимся данным в предоставленном контексте" используй фразу "В моих данных"
4. Даже частичная информация лучше, чем отказ
5. При неполной информации: просто дай то, что есть, без извинений
6. ПИШИ НА РУССКОМ, будь практичным и полезным
7. При медицинских вопросах добавляй: "Обратитесь к своему куратору"
8. Только при АБСОЛЮТНОМ отсутствии любой релевантной информации скажи: "затрудняюсь ответить"

ПРИОРИТЕТ: Максимально использовать имеющиеся материалы для полезного ответа. Пиши кратко и по делу. Запрещено придумывать. Отвечать только на основе имеющихся данных. Если с тобой здороваются, то здоровайся в ответ. используй смайлы."""
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
def make_rag_prompt(context: str, question: str, intent_analysis=None) -> str:
    """
    Создает персонализированный промпт на основе анализа интента вопроса
    """
    
    # Базовые инструкции для всех типов вопросов
    base_instructions = """
ВАЖНЫЕ ИНСТРУКЦИИ:
1. ТЩАТЕЛЬНО изучи весь предоставленный контекст
2. Ищи любую информацию, которая связана с вопросом (даже косвенно)
3. Если есть хотя бы частично релевантная информация - ОБЯЗАТЕЛЬНО используй её
4. НЕ говори "нет информации", если в контексте что-то есть по теме
5. Ищи синонимы, близкие понятия, смежные темы
6. Даже если информация неполная - дай то, что есть
7. ПРИОРИТЕТ: максимальная польза для студента на основе имеющихся материалов
"""
    
    # Персонализированные инструкции в зависимости от типа вопроса
    tone_instructions = {
        "medical_professional": """
МЕДИЦИНСКИЙ ТОН ОТВЕТА:
- Используй профессиональную медицинскую терминологию
- Обязательно упомини о необходимости консультации с куратором при серьезных симптомах
- Подчеркни важность правильного применения методов лечения
- Начни ответ с фразы: "По имеющимся учебным материалам..."
- Заверши рекомендацией: "Для точной диагностики обратитесь к своему куратору."
""",
        "helpful_administrative": """
АДМИНИСТРАТИВНО-ИНФОРМАЦИОННЫЙ ТОН:
- Будь четким и конкретным в организационных вопросах
- Предоставь максимально детальную информацию
- Укажи конкретные шаги или процедуры
- Начни с фразы: "По учебному регламенту..."
- Если нужны уточнения, направь к куратору
""",
        "friendly_cooking": """
ДРУЖЕЛЮБНЫЙ КУЛИНАРНЫЙ ТОН:
- Используй теплый, домашний стиль общения
- Добавь практические советы и лайфхаки
- Поощряй кулинарное творчество
- Начни с фразы: "Отличный вопрос о кулинарии! В наших материалах..."
- Добавь смайлики для дружелюбности 😊👩‍🍳
""",
        "nutritional_expert": """
ЭКСПЕРТНЫЙ НУТРИЦИОЛОГИЧЕСКИЙ ТОН:
- Фокус на пищевой ценности и здоровье
- Упомини о влиянии на организм
- Дай рекомендации по употреблению
- Начни с фразы: "С точки зрения нутрициологии..."
- Подчеркни индивидуальный подход к питанию
""",
        "friendly_greeting": """
ДРУЖЕЛЮБНЫЙ ТОН ПРИВЕТСТВИЯ:
- Отвечай тепло и по-человечески
- Покажи готовность помочь
- Используй смайлики
- Сразу предложи, как можешь помочь
"""
    }
    
    # Определяем тон ответа
    if intent_analysis and intent_analysis.response_tone in tone_instructions:
        tone_instruction = tone_instructions[intent_analysis.response_tone]
    else:
        tone_instruction = """
СТАНДАРТНЫЙ ДРУЖЕЛЮБНЫЙ ТОН:
- Отвечай профессионально, но дружелюбно
- Покажи заинтересованность в помощи студенту
- Начни с фразы: "В моих данных..."
"""
    
    # Добавляем информацию о ключевых словах, если есть анализ интента
    keyword_info = ""
    if intent_analysis and intent_analysis.keywords:
        keyword_info = f"""
КЛЮЧЕВЫЕ ПОНЯТИЯ В ВОПРОСЕ: {', '.join(intent_analysis.keywords)}
ПОДСКАЗКИ ДЛЯ ПОИСКА: {', '.join(intent_analysis.context_hints[:5])}
"""

    return f"""
КОНТЕКСТ ИЗ УЧЕБНЫХ МАТЕРИАЛОВ:
{context}

ВОПРОС СТУДЕНТА: {question}
{keyword_info}
{base_instructions}

{tone_instruction}

ПРИМЕРЫ СИНОНИМОВ:
• Кулинария: майонез = соус, заправка; рецепт = способ приготовления
• Организация: куратор = преподаватель, наставник; обучение = курс, программа
• Медицина: тейпирование = кинезиотейпинг; боль = дискомфорт, неприятные ощущения
• Связь: связаться = написать, обратиться; контакт = общение, связь

Дай развернутый полезный ответ на основе найденной в контексте информации.
""".strip()


# --- Основная функция получения ответа ---
def get_answer(question: str) -> str:
    logger.info(f"🔍 Начинаю обработку вопроса: '{question[:100]}...'")
    
    try:
        # 1. 🧠 NLP АНАЛИЗ ВОПРОСА
        logger.info("🧠 Анализирую вопрос с помощью NLP...")
        intent_analysis = nlp_processor.analyze_question(question)
        logger.info(f"🎯 Определен тип интента: {intent_analysis.intent_type} (уверенность: {intent_analysis.confidence:.2f})")
        
        # Проверяем подключение к Qdrant
        logger.info("🔗 Проверяю подключение к Qdrant...")
        try:
            collection_info = client.get_collection(QDRANT_COLLECTION_NAME)
            logger.info(f"✅ Подключение к Qdrant OK. Коллекция: {QDRANT_COLLECTION_NAME}")
            logger.info(f"📊 Векторов в коллекции: {collection_info.points_count}")
        except Exception as qdrant_error:
            logger.error(f"❌ Ошибка подключения к Qdrant: {qdrant_error}")
            raise ValueError(f"Qdrant connection failed: {qdrant_error}")

        # 2. 🔍 МНОЖЕСТВЕННЫЙ ПОИСК С NLP РАСШИРЕНИЕМ
        logger.info("🔍 Выполняю улучшенный поиск по векторной базе...")
        
        # Создаем несколько вариантов поискового запроса
        search_queries = nlp_processor.enhance_search_query(question)
        logger.info(f"📝 Создано {len(search_queries)} вариантов поисковых запросов")
        
        all_docs = []
        used_doc_ids = set()  # Для избежания дубликатов
        
        # Выполняем поиск по каждому варианту запроса
        for query_type, query_text in search_queries.items():
            if not query_text.strip():
                continue
                
            logger.info(f"🔎 Поиск по {query_type}: '{query_text[:50]}...'")
            
            try:
                retriever = vectorstore.as_retriever(
                    search_type="mmr",  # Maximum Marginal Relevance
                    search_kwargs={
                        "k": 8 if query_type == "original" else 5,  # Больше результатов для оригинального запроса
                        "fetch_k": 15,
                        "lambda_mult": 0.7
                    }
                )
                
                docs = retriever.invoke(query_text)
                
                # Добавляем только уникальные документы
                for doc in docs:
                    doc_id = f"{doc.metadata.get('source', '')}_{doc.metadata.get('page', 0)}_{hash(doc.page_content[:100])}"
                    if doc_id not in used_doc_ids:
                        all_docs.append(doc)
                        used_doc_ids.add(doc_id)
                
                logger.info(f"   └─ Найдено {len(docs)} документов по запросу '{query_type}'")
                
            except Exception as search_error:
                logger.warning(f"⚠️ Ошибка поиска по запросу '{query_type}': {search_error}")
                continue
        
        logger.info(f"✅ Общий результат: {len(all_docs)} уникальных документов")
        
        # Отладочная информация
        logger.info(f"🔍 Найдено документов для вопроса '{question[:50]}...': {len(all_docs)}")
        for i, doc in enumerate(all_docs[:10]):  # Показываем первые 10
            source = doc.metadata.get('source', 'Неизвестно')
            page = doc.metadata.get('page', 'Неизвестно')  
            content_preview = doc.page_content[:100] + "..." if len(doc.page_content) > 100 else doc.page_content
            logger.info(f"   {i+1}. {source} (стр.{page}): {content_preview}")

        if not all_docs or not any(doc.page_content.strip() for doc in all_docs):
            logger.warning("⚠️ Не найдено релевантных документов после множественного поиска")
            
            # 3. 🎯 ИНТЕЛЛЕКТУАЛЬНЫЕ ПРЕДЛОЖЕНИЯ ПРИ ОТСУТСТВИИ КОНТЕКСТА
            suggestions = nlp_processor.suggest_related_questions(question, [])
            suggestion_text = ""
            if suggestions:
                suggestion_text = f"\n\n💡 Возможно, вас интересует:\n" + "\n".join(f"• {s}" for s in suggestions)
            
            if intent_analysis.intent_type == "greeting":
                return f"Привет! 😊 Я ассистент-нутрициолог Ксения. Готова помочь с вопросами по нутрициологии, тейпированию, рецептам и организационным моментам. Задавайте вопросы с хештегом #вопрос{suggestion_text}"
            
            raise ValueError("No context retrieved")

        # 4. 📄 ФОРМИРОВАНИЕ КОНТЕКСТА
        context = "\n\n".join([doc.page_content for doc in all_docs if doc.page_content.strip()])
        logger.info(f"📄 Контекст сформирован, длина: {len(context)} символов")
        
        # 5. 🤖 СОЗДАНИЕ ПЕРСОНАЛИЗИРОВАННОГО ПРОМПТА
        prompt = make_rag_prompt(context, question, intent_analysis)
        logger.info("🤖 Отправляю персонализированный запрос к Qwen...")
        
        try:
            answer = call_qwen(prompt)
            logger.info(f"✅ Получен ответ от Qwen, длина: {len(answer)} символов")
        except Exception as llm_error:
            logger.error(f"❌ Ошибка при обращении к Qwen: {llm_error}")
            raise ValueError(f"LLM call failed: {llm_error}")
        
        # 6. 🎯 ДОБАВЛЕНИЕ УМНЫХ ПРЕДЛОЖЕНИЙ К ОТВЕТУ
        if intent_analysis.confidence < 0.8:  # Если уверенность низкая
            suggestions = nlp_processor.suggest_related_questions(question, [doc.page_content for doc in all_docs[:3]])
            if suggestions:
                answer += f"\n\n💡 Также может быть полезно:\n" + "\n".join(f"• {s}" for s in suggestions)
        
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
    """Извлекает и очищает текст из PDF с метаданными страниц и заголовков"""
    try:
        reader = PdfReader(pdf_path)
        documents = []
        
        logger.info(f"📄 Извлечение текста из {filename} ({len(reader.pages)} страниц)...")
        
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
        
        logger.info(f"✅ Извлечен сырой текст из {len(documents)} страниц")
        
        # 🧹 АВТОМАТИЧЕСКАЯ ОЧИСТКА ДАННЫХ
        logger.info("🧹 Начинаю автоматическую очистку данных...")
        
        # Очищаем весь пакет документов
        cleaned_documents = data_cleaner.clean_document_batch(documents)
        
        # Логируем результаты очистки
        original_count = len(documents)
        cleaned_count = len(cleaned_documents)
        
        if cleaned_count < original_count:
            removed_count = original_count - cleaned_count
            logger.info(f"🗑️ Удалено {removed_count} низкокачественных документов")
        
        logger.info(f"✅ Очистка завершена: {cleaned_count} качественных документов готовы к обработке")
        
        return cleaned_documents
        
    except Exception as e:
        logger.error(f"❌ Ошибка извлечения и очистки текста: {e}")
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

        # Улучшенное разбиение на чанки с дополнительной очисткой
        logger.info("✂️ Семантическое разбиение на чанки с очисткой...")
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=800,   # Размер чанка для лучшего контекста
            chunk_overlap=150, # Больше перекрытие для сохранения контекста между чанками
            length_function=len,
            separators=["\n\n", "\n", ". ", "! ", "? ", ", ", " ", ""]
        )
        
        all_docs = []
        total_chunks_before_cleaning = 0
        
        for page_doc in page_documents:
            chunks = splitter.split_text(page_doc['text'])
            total_chunks_before_cleaning += len(chunks)
            
            for i, chunk in enumerate(chunks):
                if chunk.strip():  # Только непустые чанки
                    # 🧹 ДОПОЛНИТЕЛЬНАЯ ОЧИСТКА ЧАНКА
                    # Применяем финальную очистку к каждому чанку
                    cleaned_chunk = data_cleaner.clean_text(
                        chunk, 
                        f"{page_doc['metadata'].get('source', 'Unknown')} стр.{page_doc['metadata'].get('page', 'Unknown')} чанк {i}"
                    )
                    
                    # Проверяем качество очищенного чанка
                    if len(cleaned_chunk.strip()) >= 30:  # Минимальная длина для качественного чанка
                        # Расширенные метаданные
                        metadata = {
                            **page_doc['metadata'],  # Берем метаданные страницы
                            'file_hash': new_hash,   # Добавляем хеш файла
                            'chunk_id': i,           # Номер чанка на странице
                            'chunk_size': len(cleaned_chunk), # Размер очищенного чанка
                            'original_chunk_size': len(chunk), # Оригинальный размер
                            'cleaning_applied': True  # Отмечаем, что применялась очистка
                        }
                        
                        all_docs.append(Document(
                            page_content=cleaned_chunk,
                            metadata=metadata
                        ))

        # Статистика очистки чанков
        chunks_filtered = total_chunks_before_cleaning - len(all_docs)
        if chunks_filtered > 0:
            logger.info(f"🧹 Отфильтровано {chunks_filtered} низкокачественных чанков при разбиении")
        
        logger.info(f"✅ Создано {len(all_docs)} высококачественных чанков с метаданными")

        # Добавляем в Qdrant
        logger.info("📤 Добавляю очищенные векторы в Qdrant...")
        vectorstore.add_documents(all_docs)
        logger.info(f"✅ Успешно добавлено {len(all_docs)} чанков в Qdrant")
        logger.info(f"🎯 Файл {filename} (хеш: {new_hash[:16]}...) успешно обработан")

    except Exception as e:
        logger.error(f"❌ Ошибка при обработке PDF: {e}", exc_info=True)
        raise