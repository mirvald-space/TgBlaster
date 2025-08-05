import sqlite3
from typing import Dict, Tuple, List, Optional

import telethon.events
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from decouple import config
from telethon import TelegramClient

# Конфиг
API_ID: int = int(config("API_ID"))
API_HASH: str = config("API_HASH")
BOT_TOKEN: str = config("BOT_TOKEN")
ADMIN_ID_LIST: List[int] = list(map(int, map(str.strip, config("ADMIN_ID_LIST").split(","))))  # <-- Вставить ID разрешенных телеграмм аккаунтов через запятую
bot: TelegramClient = TelegramClient("bot", API_ID, API_HASH)
conn: sqlite3.Connection = sqlite3.connect("sessions.db", timeout=30.0)

# Аннотирование
New_Message = telethon.events.NewMessage
Query = telethon.events.CallbackQuery
callback_query = Query.Event
callback_message = New_Message.Event
__Dict_int_str = Dict[int, str]
__Dict_all_str = Dict[str, str]
__Dict_int_dict = Dict[int, dict]


phone_waiting: Dict[int, bool] = {}  # Список пользователей ожидающие подтверждения телефона

code_waiting: __Dict_int_str = {}
broadcast_all_text: __Dict_int_str = {}
user_states: __Dict_int_str = {}

password_waiting: __Dict_int_dict = {}
broadcast_all_state: __Dict_int_dict = {}
broadcast_solo_state: __Dict_int_dict = {}
broadcast_all_state_account: __Dict_int_dict = {}
user_sessions: __Dict_int_dict = {}

user_sessions_deleting: Dict[int, __Dict_all_str] = {}
user_sessions_phone: Dict[Tuple[int, int], __Dict_all_str] = {}

user_clients: Dict[int, TelegramClient] = {}
scheduler: AsyncIOScheduler = AsyncIOScheduler()

# Словарь для отслеживания обработанных callback-запросов
processed_callbacks: Dict[str, bool] = {}


async def safe_callback_answer(event: callback_query, text: str = "") -> None:
    """
    Безопасно отвечает на callback query, обрабатывая возможные ошибки.
    """
    try:
        await event.answer(text)
    except Exception as e:
        # Игнорируем ошибки QueryIdInvalid и другие ошибки ответа на callback
        from loguru import logger
        logger.debug(f"Ошибка при ответе на callback: {e}")
        pass


def cleanup_processed_callbacks() -> None:
    """
    Очищает словарь processed_callbacks для предотвращения утечек памяти.
    Вызывается периодически через scheduler.
    """
    processed_callbacks.clear()
