from loguru import logger

from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError, PhoneCodeExpiredError, PhoneCodeInvalidError
from telethon.sessions import StringSession
from telethon.tl.functions.auth import SendCodeRequest, SignInRequest

from config import (callback_query, callback_message, phone_waiting, code_waiting, password_waiting, user_clients,
                    API_ID,
                    API_HASH, broadcast_all_state, user_states, New_Message, Query, bot, conn)


@bot.on(Query(data=b"add_account"))
async def add_account(event: callback_query) -> None:
    """
    Добавляет аккаунт
    """
    logger.info(f"Выбрана кнопка добавления аккаунта. подтверждение телефона и отправка кода")
    user_id: int = event.sender_id
    phone_waiting[user_id] = True
    await event.respond("📲 Напишите номер телефона аккаунта в формате: `+xxxxxxxxxxx`")


@bot.on(New_Message(func=lambda e: e.sender_id in phone_waiting and e.text.startswith("+") and e.text[1:].isdigit()))
async def send_code_for_phone(event: callback_message) -> None:
    """
    Отправляет код на телефон
    """
    user_id: int = event.sender_id
    phone_number: str = event.text.strip()
    logger.info(f"Отправляю {user_id} на телефон {phone_number} код подтверждения")
    user_clients[user_id] = TelegramClient(StringSession(), API_ID, API_HASH)
    await user_clients[user_id].connect()

    await event.respond("⏳ Отправляю код подтверждения...")

    try:
        await user_clients[user_id].send_code_request(phone_number)
        code_waiting[user_id] = phone_number
        del phone_waiting[user_id]
        await event.respond("✅ Код отправлен!\n\n⏰ Код действует 5-10 минут\n📱 Введите его сюда как можно скорее:")
        logger.info(f"Код отправлен")
    except Exception as e:
        if isinstance(e, (SendCodeRequest, FloodWaitError)):
            sec_time = int(str(e).split()[3])
            message = (f"⚠ Телеграмм забанил за быстрые запросы. "
                       f"Подождите {(a := sec_time // 3600)} Часов {(b := ((sec_time - a * 3600) // 60))}"
                       f" Минут {sec_time - a * 3600 - b * 60} Секунд")
            await event.respond(message)
            logger.error(message)
        else:
            phone_waiting.pop(user_id, None)
            user_clients.pop(user_id, None)
            logger.error(f"⚠ Произошла ошибка: {e}")
            await event.respond(f"⚠ Произошла ошибка: {e}\nПопробуйте снова, нажав 'Добавить аккаунт'.")


@bot.on(New_Message(
    func=lambda e: e.sender_id in code_waiting and e.text.isdigit() and e.sender_id not in broadcast_all_state))
async def get_code(event: callback_message) -> None:
    """
    Проверяет код от пользователя
    """
    code = event.text.strip()
    user_id = event.sender_id
    phone_number = code_waiting[user_id]
    cursor = conn.cursor()
    try:
        await user_clients[user_id].sign_in(phone_number, code)
        session_string = user_clients[user_id].session.save()
        me = await user_clients[user_id].get_me()
        if not cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (me.id, )).fetchall():
            cursor.execute("INSERT INTO sessions (user_id, session_string) VALUES (?, ?)", (me.id, session_string))
            conn.commit()
            await event.respond("✅ Авторизация прошла успешно!")
        else:
            await event.respond("❌ Такой аккаунт уже есть")
        del code_waiting[user_id]
        del user_clients[user_id]
    except SessionPasswordNeededError:
        password_waiting[user_id] = {"waiting": True, "last_message_id": event.message.id}
        await event.respond("⚠ Этот аккаунт защищен паролем. Отправьте пароль:")
    except PhoneCodeExpiredError as e:
        logger.error(f"Код истек: {e}")
        del code_waiting[user_id]
        user_clients.pop(user_id, None)
        await event.respond(f"⏰ Код подтверждения истек!\n\n"
                          f"Коды Telegram действуют только 5-10 минут.\n"
                          f"Нажмите 'Добавить аккаунт' чтобы получить новый код.")
    except PhoneCodeInvalidError as e:
        logger.error(f"Неверный код: {e}")
        await event.respond(f"❌ Неверный код подтверждения\n\n"
                          f"Проверьте код в SMS и введите его еще раз.\n"
                          f"Если проблема повторяется, нажмите 'Добавить аккаунт' для нового кода.")
    except Exception as e:
        del code_waiting[user_id]
        user_clients.pop(user_id, None)
        logger.error(f"Ошибка: {e}, Неверный код")
        await event.respond(f"❌ Неверный код или ошибка: {e}\nПопробуйте снова, нажав 'Добавить аккаунт'.")
    finally:
        cursor.close()


@bot.on(New_Message(func=lambda
        e: e.sender_id in password_waiting and e.sender_id not in user_states and e.sender_id not in broadcast_all_state))
async def get_password(event: callback_message) -> None:
    user_id = event.sender_id
    if password_waiting[user_id]["waiting"] and event.message.id > password_waiting[user_id]["last_message_id"]:
        password = event.text.strip()
        cursor = conn.cursor()
        try:
            await user_clients[user_id].sign_in(password=password)
            me = await user_clients[user_id].get_me()
            session_string = user_clients[user_id].session.save()

            cursor.execute("INSERT INTO sessions (user_id, session_string) VALUES (?, ?)", (me.id, session_string))
            conn.commit()

            del password_waiting[user_id]
            del user_clients[user_id]
            await event.respond("✅ Авторизация с паролем прошла успешно!")
        except Exception as e:
            await event.respond(f"⚠ Ошибка при вводе пароля: {e}\nПопробуйте снова, нажав 'Добавить аккаунт'.")
        finally:
            cursor.close()
