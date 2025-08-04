import asyncio
import logging
import os
from aiogram import Bot, Dispatcher, types, F
from config import TELEGRAM_BOT_TOKEN, TEMP_DIR
from rag import get_answer, update_knowledge_base, check_services_health

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# --- Формирование правильной ссылки на сообщение ---
def get_message_link(chat_id: int, message_id: int) -> str:
    """
    Формирует правильную ссылку на сообщение в группе/супергруппе
    """
    try:
        # Для супергрупп chat_id имеет формат -100xxxxxxxxx
        # Нужно убрать префикс "-100" для ссылки
        if str(chat_id).startswith("-100"):
            # Супергруппа - убираем "-100"
            clean_id = str(chat_id)[4:]  # Убираем "-100"
            return f"https://t.me/c/{clean_id}/{message_id}"
        else:
            # Обычная группа - используем как есть, но убираем знак минус
            clean_id = str(abs(chat_id))
            return f"https://t.me/c/{clean_id}/{message_id}"
    except Exception as e:
        logger.error(f"Ошибка создания ссылки для чата {chat_id}: {e}")
        return f"Чат ID: {chat_id}, Сообщение: {message_id}"

# --- Кнопка "Не помогло" ---
def get_didnt_help_button():
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Не помогло", callback_data="need_help")]
        ]
    )

# --- Обработка #вопрос ---
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
        # Включаем эффект "печатания" в группе
        await bot.send_chat_action(chat_id=message.chat.id, action="typing")
        
        # Получаем ответ
        answer = get_answer(question)

        # Проверяем ответы, указывающие на отсутствие информации
        no_info_phrases = [
            "затрудняюсь ответить"
        ]
        
        if any(phrase in answer.lower() for phrase in no_info_phrases):
            raise ValueError("No context")

        # Форматируем ответ
        final_answer = (
            f"📘 *Ассистент нутрициолога*\n\n"
            f"{answer}\n\n"
            f"Рекомендация дана на основе учебных материалов. "
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

        # Формируем правильную ссылку на сообщение
        message_link = get_message_link(message.chat.id, message.message_id)
        
        alert_text = (
            f"🚨 *Требуется помощь*\n\n"
            f"🔹 Группа: {chat_title}\n"
            f"🔹 Студент: {user_name}\n"
            f"🔹 Вопрос: {question_preview}\n"
            f"🔹 [Перейти к сообщению]({message_link})"
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

        # Отправляем краткое сообщение студенту
        await message.reply(
            "⚠️ К сожалению, у меня нет ответа на этот вопрос. Куратор уведомлён — скоро прибудет на помощь.",
            reply_to_message_id=message.message_id
        )

# --- Загрузка PDF в ЛС ---
@dp.message(F.document)
async def handle_pdf_upload(message: types.Message):
    logger.info("📥 handle_pdf_upload вызван")
    logger.info(f"📩 PDF получен: {message.document.file_name}")

    # Проверка: это PDF?
    if not message.document.file_name.lower().endswith(".pdf"):
        await message.reply("❌ Я принимаю только PDF-файлы.")
        return
    logger.info("✅ Расширение проверено")

    # Проверка размера
    if message.document.file_size > 10 * 1024 * 1024:
        await message.reply("❌ Файл слишком большой.")
        return
    logger.info("✅ Размер проверен")

    # Проверка прав
    upload_allowed_ids = os.getenv("ADMIN_UPLOAD_IDS", "").split(",")
    upload_allowed_ids = [int(x.strip()) for x in upload_allowed_ids if x.strip().isdigit()]
    if upload_allowed_ids and message.from_user.id not in upload_allowed_ids:
        await message.reply("❌ У вас нет прав.")
        return
    logger.info("✅ Права проверены")

    # Включаем эффект "печатания" и отправляем сообщение о начале обработки
    await bot.send_chat_action(chat_id=message.chat.id, action="typing")
    processing_message = await message.reply("⏳ Идёт обработка файла...")

    # Обработка PDF
    file_name = message.document.file_name
    file_info = await bot.get_file(message.document.file_id)
    pdf_path = os.path.join(TEMP_DIR, file_name)
    logger.info(f"📥 Скачивание файла: {file_name}")

    try:
        await bot.download_file(file_info.file_path, pdf_path)
        logger.info("✅ Файл скачан")

        update_knowledge_base(pdf_path, file_name)
        logger.info("✅ База знаний обновлена")

        # Удаляем сообщение об обработке и отправляем итоговое сообщение
        await processing_message.delete()
        await message.reply("✅ Файл успешно обновлён в базе знаний.")
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}", exc_info=True)
        # Удаляем сообщение об обработке и показываем ошибку
        try:
            await processing_message.delete()
        except:
            pass  # Если не удалось удалить сообщение, не критично
        await message.reply(f"❌ Ошибка: {str(e)[:200]}")
    finally:
        if os.path.exists(pdf_path):
            os.remove(pdf_path)
            logger.info("🗑️ Временный файл удалён")

# --- Кнопка "Не помогло" ---
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

        # Формируем правильную ссылку на сообщение
        original_message_id = callback.message.reply_to_message.message_id if callback.message.reply_to_message else callback.message.message_id
        message_link = get_message_link(callback.message.chat.id, original_message_id)
        
        alert_text = (
            f"🚨 *Требуется помощь!*\n\n"
            f"🔹 Группа: {chat_title}\n"
            f"🔹 Студент: {user_name}\n"
            f"🔹 Вопрос: {question_text}\n"
            f"🔹 [Перейти к сообщению]({message_link})"
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

# --- Запуск ---
async def main():
    logger.info("🚀 Запуск бота...")
    
    # Проверяем доступность сервисов
    if not check_services_health():
        logger.error("❌ Не удалось подключиться к сервисам. Проверьте конфигурацию.")
        return
    
    logger.info("🎉 Бот запущен. Используется Qwen через OpenRouter API.")
    await dp.start_polling(bot)

@dp.message()
async def catch_all(message: types.Message):
    logger.info(f"📩 Входящее сообщение: {message.content_type}")
    if message.document:
        logger.info(f"📄 Документ: {message.document.file_name}, MIME: {message.document.mime_type}")
    if message.photo:
        logger.info(f"🖼️ Это фото! Количество: {len(message.photo)}")
    if message.text:
        logger.info(f"💬 Текст: {message.text}")

if __name__ == "__main__":
    asyncio.run(main())
