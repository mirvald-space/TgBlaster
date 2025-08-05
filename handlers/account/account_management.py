from loguru import logger

from telethon import Button, TelegramClient
from telethon.sessions import StringSession

from config import callback_query, API_ID, API_HASH, Query, bot, conn, processed_callbacks
from utils.telegram import get_active_broadcast_groups, broadcast_status_emoji, get_entity_by_id


@bot.on(Query(data=b"my_accounts"))
async def my_accounts(event: callback_query) -> None:
    """
    Выводит список аккаунтов
    """
    try:
        cursor = conn.cursor()
        buttons = []
        accounts_found = False

        for user_id, session_string in cursor.execute("SELECT user_id, session_string FROM sessions"):
            accounts_found = True
            client = None
            try:
                client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
                await client.connect()
                me = await client.get_me()
                username = me.first_name if me.first_name else "Без ника"
                buttons.append([Button.inline(f"👤 {username}", f"account_info_{user_id}")])
            except Exception:
                buttons.append([Button.inline("⚠ Ошибка при загрузке аккаунта", f"error_{user_id}")])
            finally:
                if client:
                    await client.disconnect()

        cursor.close()

        if not accounts_found:
            await event.respond("❌ У вас нет добавленных аккаунтов")
            return

        await event.respond("📱 **Список ваших аккаунтов:**", buttons=buttons)

    except Exception as e:
        logger.error(f"Error in my_accounts: {e}")
        await event.respond("⚠ Произошла ошибка при получении списка аккаунтов")


@bot.on(Query(data=lambda data: data.decode().startswith("account_info_")))
async def handle_account_button(event: callback_query) -> None:
    # Получаем уникальный идентификатор для этого callback
    callback_id = f"{event.sender_id}:{event.query.msg_id}"
    
    # Проверяем, был ли уже обработан этот callback
    if callback_id in processed_callbacks:
        # Этот callback уже был обработан, просто возвращаемся без ответа
        return
        
    # Отмечаем callback как обработанный
    processed_callbacks[callback_id] = True
        
    user_id = int(event.data.decode().split("_")[2])
    cursor = conn.cursor()
    row = cursor.execute(
        "SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)
    ).fetchone()
    if not row:
        await event.respond("⚠ Не удалось найти аккаунт.")
        return

    session_string = row[0]
    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await client.connect()
    try:
        me = await client.get_me()
        username = me.first_name or "Без имени"
        phone = me.phone or "Не указан"
        groups = cursor.execute("SELECT group_id, group_username FROM groups WHERE user_id = ?", (user_id,))

        active_gids = get_active_broadcast_groups(user_id)
        lines = []
        
        # Получаем информацию о группах с их названиями
        for group_id, group_username in groups:
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
                                lines.append(f"{broadcast_status_emoji(user_id, int(group_id))} {display_name}")
                                continue
                        except ValueError:
                            # Если не можем преобразовать в число, пробуем использовать как есть
                            entity = await client.get_entity(group_username)
                except Exception as entity_error:
                    # Если не удалось получить entity, используем username или ID как название
                    logger.error(f"Ошибка при получении entity для группы {group_username}: {entity_error}")
                    lines.append(f"{broadcast_status_emoji(user_id, int(group_id))} {group_username}")
                    continue
                
                # Получаем название группы
                group_title = getattr(entity, 'title', group_username)
                lines.append(f"{broadcast_status_emoji(user_id, int(group_id))} {group_title}")
                
            except Exception as e:
                logger.error(f"Ошибка при обработке группы {group_id}: {e}")
                lines.append(f"{broadcast_status_emoji(user_id, int(group_id))} {group_username}")
        
        group_list = "\n".join(lines)
        if not group_list:
            group_list = "У пользователя нет групп."

        mass_active = "🟢 ВКЛ" if active_gids else "🔴 ВЫКЛ"
        buttons = [
            [
                Button.inline("📋 Список групп", f"listOfgroups_{user_id}")
            ],
            [Button.inline("🚀 Начать рассылку во все чаты", f"broadcastAll_{user_id}"),
             Button.inline("❌ Остановить общую рассылку", f"StopBroadcastAll_{user_id}")],
            [Button.inline("✔ Обновить информацию о группах", f"add_all_groups_{user_id}", )],
            [Button.inline("❌ Удалить этот аккаунт", f"delete_account_{user_id}")]
        ]

        await event.respond(
            f"📢 **Меню для аккаунта {username}:**\n"
            f"🚀 **Массовая рассылка:** {mass_active}\n\n"
            f"📌 **Имя:** {username}\n"
            f"📞 **Номер:** `+{phone}`\n\n"
            f"📝 **Список групп:**\n{group_list}",
            buttons=buttons
        )
    finally:
        await client.disconnect()
        cursor.close()
