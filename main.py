import asyncio
import logging
import os
from aiogram import Bot, Dispatcher, types, F
from config import TELEGRAM_BOT_TOKEN, TEMP_DIR
from rag import get_answer, update_knowledge_base

# Настройка логов
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Создаём бота
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

# Защита от дубликатов
last_answered = {}

# --- Получить ID админов чата ---
async def get_chat_admin_ids(chat_id: int) -> list[int]:
    try:
        admins = await bot.get_chat_administrators(chat_id)
        return [admin.user.id for admin in admins if not admin.user.is_bot]
    except Exception as e:
        logger.error(f"Не удалось получить админов чата {chat_id}: {e}")
        return []

# --- Кнопка "Не помогло" ---
def get_didnt_help_button():
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Не помогло", callback_data="need_help")]
        ]
    )

# --- ОБРАБОТКА ЗАГРУЗКИ PDF В ЛС ---
@dp.message(F.private, F.document)
async def handle_pdf_upload(message: types.Message):
    # Логируем получение файла
    logger.info(
        f"📩 Получен документ: {message.document.file_name}, "
        f"MIME: {message.document.mime_type}, "
        f"Размер: {message.document.file_size} байт"
    )

    # Проверка, что это PDF
    if message.document.mime_type != "application/pdf":
        await message.reply("❌ Я принимаю только PDF-файлы.")
        logger.warning("Файл не PDF")
        return

    # Проверка размера (макс. 10 МБ)
    MAX_SIZE = 10 * 1024 * 1024  # 10 МБ
    if message.document.file_size > MAX_SIZE:
        await message.reply("❌ Файл слишком большой. Максимум — 10 МБ.")
        logger.warning(f"Файл слишком большой: {message.document.file_size} байт")
        return

    # Проверка прав админа
    upload_allowed_ids = os.getenv("ADMIN_UPLOAD_IDS", "").split(",")
    upload_allowed_ids = [int(x.strip()) for x in upload_allowed_ids if x.strip().isdigit()]

    if upload_allowed_ids and message.from_user.id not in upload_allowed_ids:
        await message.reply("❌ У вас нет прав на загрузку PDF.")
        logger.warning(f"Нет прав: {message.from_user.id}")
        return

    # Подготовка к скачиванию
    file_name = message.document.file_name
    file_info = await bot.get_file(message.document.file_id)
    pdf_path = os.path.join(TEMP_DIR, file_name)

    try:
        # Скачиваем файл
        await bot.download_file(file_info.file_path, pdf_path)
        logger.info(f"✅ Файл скачан: {pdf_path}")

        # Обновляем базу знаний
        update_knowledge_base(pdf_path, file_name)

        # Успешный ответ
        await message.reply(
            f"✅ Файл *{file_name}* успешно обновлён в базе знаний.\n"
            f"Старая версия заменена.\n"
            f"_База знаний актуальна._",
            parse_mode="Markdown"
        )
        logger.info(f"✅ PDF '{file_name}' успешно обработан и добавлен в Pinecone")

    except Exception as e:
        logger.error(f"❌ Ошибка при обработке PDF: {e}", exc_info=True)
        await message.reply(f"❌ Ошибка при обработке файла: {str(e)[:500]}")

    finally:
        # Удаляем временный файл
        if os.path.exists(pdf_path):
            os.remove(pdf_path)
            logger.info(f"🗑️ Временный файл удалён: {pdf_path}")

# --- ОБРАБОТКА #вопрос В ГРУППАХ ---
@dp.message(F.chat.type.in_(["group", "supergroup"]))
async def handle_group_question(message: types.Message):
    if not message.text or "#вопрос" not in message.text.lower():
        return

    key = f"{message.chat.id}:{message.from_user.id}"
    if last_answered.get(key) == message.message_id:
        return
    last_answered[key] = message.message_id

    question = message.text.replace("#вопрос", "", 1).strip() or "Общий вопрос"

    try:
        # Получаем ответ
        answer = get_answer(question)

        # Если бот "затрудняется" — считаем, что контекста нет
        if "затрудняюсь" in answer.lower():
            raise ValueError("No context")

        # Форматируем ответ
        final_answer = (
            f"📘 *Ассистент нутрициолога*\n\n"
            f"{answer}\n\n"
            f"_Это рекомендация на основе учебных материалов. "
            f"Ответственность за применение несёт студент._"
        )
        await message.reply(
            final_answer,
            reply_markup=get_didnt_help_button(),
            parse_mode="Markdown",
            reply_to_message_id=message.message_id
        )

    except Exception as e:
        logger.warning(f"Нет данных для вопроса '{question}' в чате {message.chat.id}: {e}")

        chat_title = message.chat.title or f"Чат ID: {message.chat.id}"
        user_name = message.from_user.full_name
        question_preview = (question[:150] + "...") if len(question) > 150 else question

        alert_text = (
            f"🚨 *Требуется помощь*\n\n"
            f"🔹 Группа: {chat_title}\n"
            f"🔹 Студент: {user_name}\n"
            f"🔹 Вопрос: {question_preview}\n"
            f"🔹 [Перейти к сообщению](https://t.me/c/{message.chat.id}/{message.message_id})"
        )

        chat_admin_ids = await get_chat_admin_ids(message.chat.id)
        notified_count = 0
        for admin_id in chat_admin_ids:
            try:
                await bot.send_message(admin_id, alert_text, parse_mode="Markdown")
                notified_count += 1
            except Exception as err:
                logger.error(f"Не удалось уведомить {admin_id}: {err}")

        logger.info(f"Уведомлено {notified_count} админов группы {chat_title}")

        await message.reply(
            "Я затрудняюсь ответить на этот вопрос. "
            "Куратор вашей группы уже уведомлён — помощь будет предоставлена.",
            reply_to_message_id=message.message_id
        )

# --- ОБРАБОТКА КНОПКИ "НЕ ПОМОГЛО" ---
@dp.callback_query(F.data == "need_help")
async def handle_need_help(callback: types.CallbackQuery):
    try:
        chat_admin_ids = await get_chat_admin_ids(callback.message.chat.id)
        chat_title = callback.message.chat.title or f"Чат ID: {callback.message.chat.id}"
        user_name = callback.from_user.full_name
        question_text = (
            callback.message.reply_to_message.text[:200] + "..."
            if callback.message.reply_to_message and callback.message.reply_to_message.text
            else "Неизвестно"
        )

        alert_text = (
            f"🚨 *Требуется помощь!*\n\n"
            f"🔹 Группа: {chat_title}\n"
            f"🔹 Студент: {user_name}\n"
            f"🔹 Вопрос: {question_text}\n"
            f"🔹 [Перейти к сообщению](https://t.me/c/{callback.message.chat.id}/{callback.message.message_id})"
        )

        notified_count = 0
        for admin_id in chat_admin_ids:
            try:
                await bot.send_message(admin_id, alert_text, parse_mode="Markdown")
                notified_count += 1
            except Exception as e:
                logger.error(f"Ошибка при уведомлении {admin_id}: {e}")

        logger.info(f"Кнопка 'Не помогло': уведомлено {notified_count} админов")

        await callback.message.edit_text(
            callback.message.html_text + "\n\n⚠️ Куратор уведомлён — помощь будет предоставлена.",
            parse_mode="HTML"
        )
        await callback.answer("Куратор уведомлён.")
    except Exception as e:
        logger.error(f"Ошибка в 'need_help': {e}")
        await callback.answer("Не удалось отправить уведомление.", show_alert=True)

# --- ЗАПУСК БОТА ---
async def main():
    logger.info("Бот запущен. Используется Qwen через dashscope.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
