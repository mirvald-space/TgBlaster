from loguru import logger
import sqlite3
import os

from telethon import Button, TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest

from config import callback_query, API_ID, API_HASH, broadcast_all_text, scheduler, Query, bot, conn
from utils.telegram import gid_key, broadcast_status_emoji, get_entity_by_id


@bot.on(Query(data=lambda data: data.decode().startswith("listOfgroups_")))
async def handle_groups_list(event: callback_query) -> None:
    user_id = int(event.data.decode().split("_")[1])
    cursor = conn.cursor()
    row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()
    if not row:
        await event.respond("⚠ Не удалось найти аккаунт.")
        return

    session_string = row[0]
    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await client.connect()
    try:
        dialogs = cursor.execute("SELECT group_id, group_username FROM groups WHERE user_id = ?", (user_id,))
        buttons = []
        
        for dialog in dialogs:
            group_id, group_username = dialog
            try:
                # Пытаемся получить entity группы
                try:
                    # Проверяем, является ли group_username числом (ID группы) или именем пользователя
                    if group_username.startswith('@'):
                        # Это username группы
                        entity = await client.get_entity(group_username)
                    else:
                        # Пробуем получить entity по ID
                        try:
                            group_id_int = int(group_username)
                            entity = await get_entity_by_id(client, group_id_int)
                            if not entity:
                                # Если не удалось получить entity, используем ID как название
                                display_name = f"Группа с ID {group_id}"
                                buttons.append(
                                    [Button.inline(f"{broadcast_status_emoji(user_id, int(group_id))} {display_name}",
                                                f"groupInfo_{user_id}_{gid_key(group_id)}".encode())]
                                )
                                continue
                        except ValueError:
                            # Если не можем преобразовать в число, пробуем использовать как есть
                            entity = await client.get_entity(group_username)
                except Exception as entity_error:
                    # Если не удалось получить entity, используем username или ID как название
                    logger.error(f"Ошибка при получении entity для группы {group_username}: {entity_error}")
                    buttons.append(
                        [Button.inline(f"{broadcast_status_emoji(user_id, int(group_id))} {group_username}",
                                    f"groupInfo_{user_id}_{gid_key(group_id)}".encode())]
                    )
                    continue
                
                # Получаем название группы
                group_title = getattr(entity, 'title', group_username)
                buttons.append(
                    [Button.inline(f"{broadcast_status_emoji(user_id, int(group_id))} {group_title}",
                                f"groupInfo_{user_id}_{gid_key(group_id)}".encode())]
                )
                
            except Exception as e:
                logger.error(f"Ошибка при обработке группы {group_id}: {e}")
                buttons.append(
                    [Button.inline(f"{broadcast_status_emoji(user_id, int(group_id))} {group_username}",
                                f"groupInfo_{user_id}_{gid_key(group_id)}".encode())]
                )
        
        cursor.close()
        if not buttons:
            await event.respond("У аккаунта нет групп.")
            return

        await event.respond("📋 Список групп, в которых вы состоите:", buttons=buttons)
    finally:
        await client.disconnect()


# ---------- меню конкретной группы ----------
@bot.on(Query(data=lambda d: d.decode().startswith("groupInfo_")))
async def group_info(event: callback_query) -> None:
    data = event.data.decode()
    user_id, group_id = map(int, data.split("_")[1:])
    cursor = conn.cursor()
    
    # Проверяем наличие сессии
    session_row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()
    if not session_row:
        await event.respond("⚠ Ошибка: не найдена сессия для этого аккаунта.")
        cursor.close()
        return
        
    session_string = session_row[0]
    session = StringSession(session_string)
    client = TelegramClient(session, API_ID, API_HASH)
    
    # Проверяем наличие группы
    group_row = cursor.execute("SELECT group_username FROM groups WHERE user_id = ? AND group_id = ?", 
                             (user_id, group_id)).fetchone()
    if not group_row:
        await event.respond("⚠ Ошибка: не найдена группа.")
        cursor.close()
        return
        
    group_username = group_row[0]
    
    try:
        await client.connect()
        
        try:
            ent = await client.get_entity(group_row[0])
        except Exception as entity_error:
            if "Cannot find any entity corresponding to" in str(entity_error):
                try:
                    # Преобразуем username в ID, если это возможно
                    try:
                        group_id_int = int(group_row[0])
                        ent = await get_entity_by_id(client, group_id_int)
                        if not ent:
                            await event.respond(f"⚠ Ошибка: не удалось получить информацию о группе {group_username}.")
                            await client.disconnect()
                            cursor.close()
                            return
                    except ValueError:
                        # Если username не является числом, сообщаем об ошибке
                        await event.respond(f"⚠ Ошибка: не удалось получить информацию о группе {group_username}.")
                        await client.disconnect()
                        cursor.close()
                        return
                except Exception as alt_error:
                    logger.error(f"Ошибка при альтернативном получении Entity: {alt_error}")
                    await event.respond(f"⚠ Ошибка: не удалось получить информацию о группе {group_username}.")
                    await client.disconnect()
                    cursor.close()
                    return
            else:
                await event.respond(f"⚠ Ошибка: не удалось получить информацию о группе {group_username}.")
                await client.disconnect()
                cursor.close()
                return
        
        # Получаем информацию о рассылке
        broadcast_row = cursor.execute("""
            SELECT broadcast_text, interval_minutes, is_active, photo_url 
            FROM broadcasts 
            WHERE user_id = ? AND group_id = ?
        """, (user_id, gid_key(group_id))).fetchone()
        
        broadcast_text = broadcast_row[0] if broadcast_row and broadcast_row[0] else "Не установлен"
        interval = f"{broadcast_row[1]} мин." if broadcast_row and broadcast_row[1] else "Не установлен"
        status = broadcast_status_emoji(user_id, group_id)
        
        # Получаем информацию о фото
        photo_url = broadcast_row[3] if broadcast_row and len(broadcast_row) > 3 and broadcast_row[3] else None
        photo_info = f"Фото: {os.path.basename(photo_url)}" if photo_url else "Фото отсутствует"
        
        # Формируем информацию о группе
        group_title = getattr(ent, 'title', group_username)
        group_username_display = f"@{ent.username}" if hasattr(ent, 'username') and ent.username else "Нет юзернейма"
        
        # Получаем количество участников с обработкой None
        members_count = getattr(ent, 'participants_count', None)
        if members_count is None:
            try:
                # Для супергрупп и каналов пытаемся получить количество участников через полную информацию
                if isinstance(ent, Channel):
                    full_channel = await client(GetFullChannelRequest(ent))
                    members_count = getattr(full_channel.full_chat, 'participants_count', "Неизвестно")
                elif isinstance(ent, Chat):
                    full_chat = await client(GetFullChatRequest(ent.id))
                    members_count = getattr(full_chat.full_chat, 'participants_count', "Неизвестно")
                else:
                    members_count = "Неизвестно"
            except Exception as e:
                logger.error(f"Не удалось получить количество участников: {e}")
                members_count = "Неизвестно"
        
        if isinstance(ent, Channel):
            group_type = "Канал" if ent.broadcast else "Супергруппа"
        elif isinstance(ent, Chat):
            group_type = "Группа"
        else:
            group_type = "Неизвестный тип"
        
        info_text = f"""
📊 **Информация о группе**

👥 **Название**: {group_title}
🔖 **Юзернейм**: {group_username_display}
👤 **Участников**: {members_count}
📝 **Тип**: {group_type}
🆔 **ID**: {group_id}

📬 **Статус рассылки**: {status}
⏱ **Интервал**: {interval}
📝 **Текст рассылки**: 
{broadcast_text[:100] + '...' if len(broadcast_text) > 100 else broadcast_text}
🖼 **{photo_info}**
"""
        
        # Кнопки для управления рассылкой
        buttons = [
            [Button.inline(f"📝 Текст и Интервал рассылки", f"BroadcastTextInterval_{user_id}_{group_id}".encode())],
            [Button.inline(f"▶️ Начать/возобновить рассылку", f"StartResumeBroadcast_{user_id}_{group_id}".encode())],
            [Button.inline(f"⏹ Остановить рассылку", f"StopAccountBroadcast_{user_id}_{group_id}".encode())],
            [Button.inline(f"❌ Удалить группу", f"DeleteGroup_{user_id}_{group_id}".encode())],
            [Button.inline(f"◀️ Назад к списку групп", f"groups_{user_id}".encode())]
        ]
        
        await event.respond(info_text, buttons=buttons)
        
    except Exception as e:
        logger.error(f"Ошибка при получении информации о группе: {e}")
        await event.respond(f"⚠ Ошибка при получении информации о группе: {str(e)}")
    finally:
        await client.disconnect()
        cursor.close()
