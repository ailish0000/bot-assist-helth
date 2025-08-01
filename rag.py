from langchain_pinecone import PineconeVectorStore
from langchain_huggingface import HuggingFaceEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from PyPDF2 import PdfReader
import dashscope  # Официальная библиотека Qwen
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Устанавливаем API-ключ Qwen
dashscope.api_key = os.getenv("QWEN_API_KEY")

# Эмбеддинги
embeddings = HuggingFaceEmbeddings(model_name=os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"))

# Векторная база
vectorstore = PineconeVectorStore(
    index_name=os.getenv("PINECONE_INDEX_NAME", "nutri-knowledge"),
    embedding=embeddings
)

# --- Функция: вызов Qwen ---
def call_qwen(prompt: str) -> str:
    try:
        response = dashscope.Generation.call(
            model="qwen-max",  # можно заменить на qwen-turbo
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
        logger.info(f"🗑️ Удаление старой версии {filename} из Pinecone...")
        vectorstore.delete(filter={"source": filename})
        logger.info("✅ Старая версия удалена")

        # Читаем PDF
        logger.info("📖 Открываем PDF...")
        reader = PdfReader(pdf_path)
        text = ""
        page_count = len(reader.pages)
        logger.info(f"Найдено страниц: {page_count}")

        for i, page in enumerate(reader.pages):
            logger.info(f"Читаю страницу {i+1}...")
            extracted = page.extract_text()
            if extracted:
                text += extracted + "\n"
            else:
                logger.warning(f"Страница {i+1} — текст не извлечён")

        if not text.strip():
            raise ValueError("❌ PDF не содержит читаемого текста")

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

        # Добавляем в Pinecone
        logger.info("📤 Добавляю векторы в Pinecone...")
        vectorstore.add_documents(docs)
        logger.info(f"✅ Успешно добавлено {len(chunks)} чанков в Pinecone")

    except Exception as e:
        logger.error(f"❌ Ошибка при обработке PDF: {e}", exc_info=True)
        raise

        # Читаем PDF
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            extracted = page.extract_text()
            if extracted:
                text += extracted + "\n"

        if not text.strip():
            raise ValueError("PDF не содержит читаемого текста")

        # Разбиваем на чанки
        splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
        chunks = splitter.split_text(text)

        # Подготовка документов
        docs = [
            {"page_content": chunk, "metadata": {"source": filename}}
            for chunk in chunks
        ]

        # Добавляем в Pinecone
        vectorstore.add_documents(docs)
        logger.info(f"Добавлено {len(chunks)} чанков из {filename}")

    except Exception as e:
        logger.error(f"Ошибка обновления базы: {e}")
        raise
