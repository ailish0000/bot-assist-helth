from langchain_huggingface import HuggingFaceEmbeddings
from langchain_community.vectorstores import Qdrant
from langchain.text_splitter import RecursiveCharacterTextSplitter
from PyPDF2 import PdfReader
import dashscope
import logging
import os
from qdrant_client import QdrantClient
from qdrant_client.http import models

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Устанавливаем API-ключ Qwen
dashscope.api_key = os.getenv("QWEN_API_KEY")

# --- Конфигурация Qdrant ---
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_COLLECTION_NAME = os.getenv("QDRANT_COLLECTION_NAME", "nutri-knowledge")

if not QDRANT_URL or not QDRANT_API_KEY:
    logger.error("❗ Не заданы QDRANT_URL или QDRANT_API_KEY")
    raise ValueError("Qdrant: URL и API-ключ обязательны")

# --- Эмбеддинги ---
embeddings = HuggingFaceEmbeddings(model_name=os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"))

# --- Клиент Qdrant ---
client = QdrantClient(
    url=QDRANT_URL,
    api_key=QDRANT_API_KEY,
    timeout=30
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

# --- Векторное хранилище ---
vectorstore = Qdrant(
    client=client,
    collection_name=QDRANT_COLLECTION_NAME,
    embeddings=embeddings
)

# --- Функция: вызов Qwen ---
def call_qwen(prompt: str) -> str:
    try:
        response = dashscope.Generation.call(
            model="qwen-max",
            prompt=prompt,
            max_tokens=512,
            temperature=0.5
        )
        if response.status_code == 200:
            return response.output["text"].strip()
        else:
            logger.error(f"Qwen API error {response.status_code}: {response.message}")
            return "Извините, сейчас не могу сформировать ответ."
    except Exception as e:
        logger.error(f"Ошибка при вызове Qwen: {e}")
        return "Ошибка при генерации ответа."

# --- Формирование RAG-промпта ---
def make_rag_prompt(context: str, question: str) -> str:
    return f"""
Вы — ассистент нутрициолога. Отвечайте кратко, вежливо и только на основе следующего контекста.
Если ответа нет в контексте, скажите: "Я затрудняюсь ответить на этот вопрос. Куратор уже уведомлён."

Контекст:
{context}

Вопрос: {question}

Ответ:
""".strip()

# --- Основная функция получения ответа ---
def get_answer(question: str) -> str:
    try:
        # Получаем релевантные чанки
        retriever = vectorstore.as_retriever(search_kwargs={"k": 3})
        docs = retriever.invoke(question)
        
        if not docs or not any(doc.page_content.strip() for doc in docs):
            raise ValueError("No context retrieved")

        context = "\n\n".join([doc.page_content for doc in docs if doc.page_content.strip()])
        prompt = make_rag_prompt(context, question)
        answer = call_qwen(prompt)
        return answer
    except Exception as e:
        logger.error(f"Ошибка генерации ответа: {e}")
        return "Извините, произошла ошибка при обработке запроса."

# --- Обновление базы знаний ---
def update_knowledge_base(pdf_path: str, filename: str):
    try:
        logger.info(f"🔄 Начало обработки PDF: {filename}")

        # Удаляем старую версию
        logger.info(f"🗑️ Удаление старой версии {filename} из Qdrant...")
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
        else:
            logger.info(f"ℹ️ Файл {filename} не найден в базе — пропускаем удаление")

        # Читаем PDF
        reader = PdfReader(pdf_path)
        text = ""
        for i, page in enumerate(reader.pages):
            extracted = page.extract_text()
            if extracted:
                text += extracted + "\n"
            else:
                logger.warning(f"Страница {i+1} — текст не извлечён")

        if not text.strip():
            raise ValueError("PDF не содержит текста")

        logger.info(f"✅ Текст извлечён. Длина: {len(text)} символов")

        # Разбиваем на чанки
        logger.info("✂️ Разбиваю на чанки...")
        splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
        chunks = splitter.split_text(text)
        logger.info(f"✅ Разбито на {len(chunks)} чанков")

        # Подготовка документов
        docs = [
            {"page_content": chunk, "metadata": {"source": filename}}
            for chunk in chunks
        ]

        # Добавляем в Qdrant
        logger.info("📤 Добавляю векторы в Qdrant...")
        vectorstore.add_documents(docs)
        logger.info(f"✅ Успешно добавлено {len(chunks)} чанков в Qdrant")

    except Exception as e:
        logger.error(f"❌ Ошибка при обработке PDF: {e}", exc_info=True)
        raise
