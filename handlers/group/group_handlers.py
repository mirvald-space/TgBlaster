from sqlite3 import IntegrityError

from config import callback_query, callback_message, user_sessions, New_Message, Query, bot, conn


@bot.on(Query(data=b"add_groups"))
async def manage_groups(event: callback_query) -> None:
    user_sessions[event.sender_id] = {"step": "awaiting_group_username"}
    await event.respond("📲 Напишите @username группы или ID группы, чтобы добавить её в базу данных:")


@bot.on(New_Message(func=lambda event: (user_state := user_sessions.pop(event.sender_id, None)) and user_state[
    "step"] == "awaiting_group_username"))
async def handle_group_input(event: callback_message) -> None:
    group_identifier: str = event.text.strip()

    # Проверяем, является ли ввод username или ID
    if group_identifier.startswith("@") and " " not in group_identifier:
        # Это username группы
        cursor = conn.cursor()
        try:
            ids = await bot.get_entity(group_identifier)
            cursor.execute("INSERT INTO pre_groups (group_username, group_id) VALUES (?, ?)",
                           (group_identifier, ids.id))
            conn.commit()
            await event.respond(f"✅ Группа {group_identifier} успешно добавлена в базу данных!")
        except IntegrityError:
            await event.respond("⚠ Эта группа уже существует в базе данных.")
        except Exception as e:
            await event.respond(f"⚠ Ошибка при добавлении группы: {str(e)}")
        finally:
            cursor.close()
    elif group_identifier.isdigit():
        # Это ID группы
        cursor = conn.cursor()
        try:
            group_id = int(group_identifier)
            # Сохраняем ID как строку для приватных групп
            cursor.execute("INSERT INTO pre_groups (group_username, group_id) VALUES (?, ?)",
                           (group_identifier, group_id))
            conn.commit()
            await event.respond(f"✅ Группа с ID {group_identifier} успешно добавлена в базу данных!")
        except IntegrityError:
            await event.respond("⚠ Эта группа уже существует в базе данных.")
        except Exception as e:
            await event.respond(f"⚠ Ошибка при добавлении группы: {str(e)}")
        finally:
            cursor.close()
    else:
        await event.respond("⚠ Ошибка! Неправильный формат. Введите @username группы или числовой ID группы.")
