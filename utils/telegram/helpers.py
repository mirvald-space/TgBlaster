from loguru import logger
from typing import List, Union, Optional

from telethon.tl.types import Channel, Chat, PeerChannel, PeerChat, InputPeerChannel, InputPeerChat
from telethon import TelegramClient

from config import conn


def gid_key(value: int) -> int:
    """Возвращает abs(id).  Для супергрупп (-100...) и обычных чатов получается один и тот же «ключ»."""
    return abs(value)


def broadcast_status_emoji(user_id: int,
                           group_id: int) -> str:
    gid_key_str = gid_key(group_id)
    return "✅ Активна" if gid_key_str in get_active_broadcast_groups(user_id) else "❌ Законченна или не начата"


def get_active_broadcast_groups(user_id: int) -> List[int]:
    active = set()
    cursor = conn.cursor()
    cursor.execute("""SELECT group_id FROM broadcasts WHERE is_active = ? AND user_id = ?""", (True, user_id))
    broadcasts = cursor.fetchall()
    for job in broadcasts:
        active.add(job[0])
    cursor.close()
    return list(active)


def create_broadcast_data(user_id: int,
                      group_id: int,
                      text: str,
                      interval_minutes: int,
                      photo_url: str = None) -> None:
    """Создает или обновляет запись в таблице broadcasts."""
    cursor = conn.cursor()

    # Используем gid_key для правильной обработки ID группы
    group_id_key = gid_key(group_id)

    # Проверяем наличие записи
    cursor.execute("""SELECT * FROM broadcasts WHERE user_id = ? AND group_id = ?""", (user_id, group_id_key))
    if cursor.fetchone():
        # Обновляем существующую запись
        cursor.execute("""UPDATE broadcasts 
                          SET broadcast_text = ?, interval_minutes = ?, is_active = ?, photo_url = ?
                          WHERE user_id = ? AND group_id = ?""",
                       (text, interval_minutes, True, photo_url, user_id, group_id_key))
    else:
        # Создаем новую запись
        cursor.execute("""INSERT INTO broadcasts 
                          (user_id, group_id, broadcast_text, interval_minutes, is_active, photo_url) 
                          VALUES (?, ?, ?, ?, ?, ?)""",
                       (user_id, group_id_key, text, interval_minutes, True, photo_url))

    conn.commit()
    cursor.close()


async def get_entity_by_id(client: TelegramClient, group_id: int) -> Optional[Union[Channel, Chat]]:
    """
    Пытается получить объект группы по ID, используя разные методы.
    
    Args:
        client: Экземпляр TelegramClient
        group_id: ID группы
    
    Returns:
        Объект Channel или Chat, если удалось получить, иначе None
    """
    try:
        # Пробуем получить как канал (большинство групп в Telegram - это каналы)
        try:
            entity = await client.get_entity(PeerChannel(group_id))
            return entity
        except Exception as e:
            logger.debug(f"Не удалось получить как PeerChannel: {e}")
            
        # Пробуем получить как обычный чат
        try:
            entity = await client.get_entity(PeerChat(group_id))
            return entity
        except Exception as e:
            logger.debug(f"Не удалось получить как PeerChat: {e}")
            
        # Пробуем через InputPeer
        try:
            entity = await client.get_entity(InputPeerChannel(group_id, 0))
            return entity
        except Exception as e:
            logger.debug(f"Не удалось получить как InputPeerChannel: {e}")
            
        try:
            entity = await client.get_entity(InputPeerChat(group_id))
            return entity
        except Exception as e:
            logger.debug(f"Не удалось получить как InputPeerChat: {e}")
            
        # Пробуем напрямую по ID
        try:
            entity = await client.get_entity(group_id)
            return entity
        except Exception as e:
            logger.debug(f"Не удалось получить напрямую по ID: {e}")
            
        logger.error(f"Не удалось получить entity для group_id={group_id}")
        return None
    except Exception as e:
        logger.error(f"Ошибка при получении entity: {e}")
        return None
