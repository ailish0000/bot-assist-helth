from langchain_pinecone import PineconeVectorStore
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.llms import Qwen
from langchain.chains import RetrievalQA
from langchain.text_splitter import RecursiveCharacterTextSplitter
from PyPDF2 import PdfReader
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    embeddings = HuggingFaceEmbeddings(model_name=os.getenv("EMBEDDING_MODEL"))
    llm = Qwen(
        model_name=os.getenv("QWEN_MODEL"),
        dashscope_api_key=os.getenv("QWEN_API_KEY")
    )
    vectorstore = PineconeVectorStore(
        index_name=os.getenv("PINECONE_INDEX_NAME"),
        embedding=embeddings
    )
    qa_chain = RetrievalQA.from_chain_type(
        llm=llm,
        retriever=vectorstore.as_retriever(search_kwargs={"k": 3}),
        chain_type="stuff",
        return_source_documents=True
    )
except Exception as e:
    logger.error(f"Ошибка инициализации RAG: {e}")
    raise

def update_knowledge_base(pdf_path: str, filename: str):
    try:
        vectorstore.delete(filter={"source": filename})
        logger.info(f"Удалено: {filename}")

        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            extracted = page.extract_text()
            if extracted:
                text += extracted + "\n"

        if not text.strip():
            raise ValueError("PDF не содержит читаемого текста")

        splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
        chunks = splitter.split_text(text)

        docs = [
            {"page_content": chunk, "metadata": {"source": filename}}
            for chunk in chunks
        ]
        vectorstore.add_documents(docs)
        logger.info(f"Добавлено {len(chunks)} чанков из {filename}")

    except Exception as e:
        logger.error(f"Ошибка обновления базы: {e}")
        raise
