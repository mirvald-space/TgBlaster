import asyncio
import datetime
from loguru import logger
from typing import Union, Optional

from apscheduler.triggers.interval import IntervalTrigger
from telethon import TelegramClient
from telethon.errors import ChatWriteForbiddenError, ChatAdminRequiredError, FloodWaitError, SlowModeWaitError
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat
from telethon.tl.custom import Button

from config import callback_query, API_ID, API_HASH, scheduler, Query, bot, conn, New_Message, \
    broadcast_solo_state, callback_message
from utils.telegram import gid_key, get_entity_by_id, create_broadcast_data
from utils.logging import log_message_event, log_user_action


async def send_broadcast_message(user_id: int, group_id: int, text: str, session_string: str, photo_url: Optional[str] = None, max_retries: int = 10) -> None:
    """
    Отправляет сообщение рассылки в группу с обработкой ошибок и повторными попытками.
    
    Args:
        user_id: ID пользователя (владельца аккаунта)
        group_id: ID группы для отправки
        text: Текст сообщения
        session_string: Строка сессии Telethon
        photo_url: Опциональный путь к фото для отправки
        max_retries: Максимальное количество попыток отправки
    """
    retry_count = 0
    job_id = f"broadcast_{user_id}_{gid_key(group_id)}"
    
    while retry_count < max_retries:
        try:
            async with TelegramClient(StringSession(session_string), API_ID, API_HASH) as client:
                with conn:
                    cursor = conn.cursor()
                    # Получаем актуальный текст рассылки и фото из базы данных
                    cursor.execute("""SELECT broadcast_text, photo_url FROM broadcasts 
                                    WHERE group_id = ? AND user_id = ?""",
                                   (gid_key(group_id), user_id))
                    current_data = cursor.fetchone()
                    txt = current_data[0] if current_data and current_data[0] else text
                    photo_url_from_db = current_data[1] if current_data and len(current_data) > 1 else None
                    
                    # Определяем, использовать ли фото из базы данных или отправлять новое
                    photo_to_send = photo_url_from_db if photo_url_from_db else photo_url
                    
                    # Получаем информацию о группе
                    try:
                        # Пробуем получить entity группы
                        group_entity = await get_entity_by_id(client, group_id)
                        
                        if not group_entity:
                            # Если не удалось получить entity, проверяем наличие username в базе
                            group_row = cursor.execute("SELECT group_username FROM groups WHERE user_id = ? AND group_id = ?", 
                                                    (user_id, group_id)).fetchone()
                            
                            if group_row and group_row[0]:
                                group_username = group_row[0]
                                if group_username.startswith('@'):
                                    # Это username группы
                                    group_entity = await client.get_entity(group_username)
                                else:
                                    # Пробуем получить entity по ID
                                    try:
                                        group_id_int = int(group_username)
                                        group_entity = await get_entity_by_id(client, group_id_int)
                                    except ValueError:
                                        # Если не можем преобразовать в число, пробуем использовать как есть
                                        group_entity = await client.get_entity(group_username)
                        
                        # Попытка отправить сообщение
                        if photo_to_send:
                            try:
                                # Отправляем сообщение с фото
                                await client.send_file(group_entity, photo_to_send, caption=txt)
                                logger.debug(f"Отправлено сообщение с фото в {getattr(group_entity, 'title', 'группу')}")
                            except Exception as photo_error:
                                logger.error(f"Ошибка при отправке с фото: {photo_error}")
                                # Если не удалось отправить с фото, пробуем отправить только текст
                                await client.send_message(group_entity, txt)
                                logger.debug(f"Отправлено сообщение без фото в {getattr(group_entity, 'title', 'группу')}")
                        else:
                            # Отправляем обычное текстовое сообщение
                            await client.send_message(group_entity, txt)
                            logger.debug(f"Успешно отправлено в {getattr(group_entity, 'title', 'группу')}")
                        
                        # Записываем в историю отправок
                        cursor.execute("""INSERT INTO send_history 
                                        (user_id, group_id, group_name, sent_at, message_text) 
                                        VALUES (?, ?, ?, ?, ?)""",
                                       (user_id, group_id, getattr(group_entity, 'title', ''),
                                        datetime.datetime.now().isoformat(), txt))
                        
                        # Обновляем статус рассылки
                        cursor.execute("""UPDATE broadcasts 
                                        SET error_reason = NULL 
                                        WHERE user_id = ? AND group_id = ?""",
                                       (user_id, gid_key(group_id)))
                        conn.commit()
                        return  # Успешно отправлено, выходим из функции
                        
                    except (ChatWriteForbiddenError, ChatAdminRequiredError) as e:
                        error_msg = f"Нет прав писать в группу: {e}"
                        logger.error(error_msg)
                        cursor.execute("""UPDATE broadcasts 
                                        SET is_active = ?, error_reason = ? 
                                        WHERE user_id = ? AND group_id = ?""",
                                       (False, error_msg, user_id, gid_key(group_id)))
                        conn.commit()
                        if scheduler.get_job(job_id):
                            scheduler.remove_job(job_id)
                        return  # Нет смысла повторять, выходим из функции
                        
                    except Exception as entity_error:
                        # Проверяем, не связана ли ошибка с невозможностью найти entity
                        if "Cannot find any entity corresponding to" in str(entity_error):
                            logger.error(f"Не удалось найти группу: {entity_error}")
                            error_msg = f"Не удалось найти группу: {entity_error}"
                            cursor.execute("""UPDATE broadcasts 
                                            SET is_active = ?, error_reason = ? 
                                            WHERE user_id = ? AND group_id = ?""",
                                           (False, error_msg, user_id, gid_key(group_id)))
                            conn.commit()
                            if scheduler.get_job(job_id):
                                scheduler.remove_job(job_id)
                            return  # Нет смысла повторять, выходим из функции
                        else:
                            raise entity_error  # Другие ошибки пробрасываем дальше
        
        except (FloodWaitError, SlowModeWaitError) as e:
            wait_time = e.seconds
            logger.warning(f"{type(e).__name__}: ожидание {wait_time} сек.")
            await asyncio.sleep(wait_time + 10)
            retry_count += 1
        except Exception as e:
            logger.error(f"Ошибка при отправке: {type(e).__name__}: {e}")
            retry_count += 1
            await asyncio.sleep(5)
    
    # Если достигли максимального количества попыток
    logger.warning(f"Не удалось отправить сообщение после {max_retries} попыток")
    with conn:
        cursor = conn.cursor()
        error_msg = f"Не удалось отправить после {max_retries} попыток"
        cursor.execute("""UPDATE broadcasts 
                        SET is_active = ?, error_reason = ? 
                        WHERE user_id = ? AND group_id = ?""",
                       (False, error_msg, user_id, gid_key(group_id)))
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)


@bot.on(Query(data=lambda d: d.decode().startswith("BroadcastTextInterval_")))
async def same_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
    data = event.data.decode()
    user_id, group_id = map(int, data.split("_")[1:])
    broadcast_solo_state[admin_id] = {"user_id": user_id, "mode": "same", "step": "text", "group_id": group_id}
    await event.respond("📝 Пришлите текст рассылки для **одной** группы этого аккаунта:")


# ---------- мастер-диалог (текст → интервалы) ----------
@bot.on(New_Message(func=lambda e: e.sender_id in broadcast_solo_state))
async def broadcast_all_dialog(event: callback_message) -> None:
    st = broadcast_solo_state[event.sender_id]
    log_message_event(event, "обработка диалога индивидуальной рассылки")
    # шаг 1 — получили текст
    if st["step"] == "text":
        st["text"] = event.text
        st["step"] = "interval"
        await event.respond("⏲️ Введите интервал (минуты, одно число):")
        return

    # шаг 2 - получили интервал
    if st["step"] == "interval":
        try:
            min_time = int(event.text)
        except ValueError:
            await event.respond(f"Некорректный формат числа. попробуйте еще раз нажав /start")
            return
        if min_time <= 0:
            await event.respond("⚠ Должно быть положительное число.")
            return
        
        # Добавляем шаг для выбора прикрепления фото
        st["interval"] = min_time
        st["step"] = "photo_choice"
        
        # Создаем кнопки для выбора
        buttons = [
            [Button.inline("✅ Да, прикрепить фото", b"photo_yes")],
            [Button.inline("📸 Только изображение", b"photo_only")],
            [Button.inline("❌ Нет, только текст", b"photo_no")]
        ]
        
        await event.respond("📸 Хотите прикрепить фото к сообщению?", buttons=buttons)
        return
        
    # шаг 3 - получили фото (если пользователь выбрал "Да" или "Только изображение")
    if st["step"] == "photo" or st["step"] == "photo_only":
        if event.photo:
            # Если пользователь отправил фото
            try:
                # Скачиваем фото
                photo = await event.download_media()
                st["photo_url"] = photo
                
                # Запускаем рассылку
                job_id = f"broadcast_{st['user_id']}_{gid_key(st['group_id'])}"
                
                # Обновляем данные рассылки в базе
                create_broadcast_data(st["user_id"], st["group_id"], st["text"], st["interval"], photo)
                
                # Проверяем, есть ли уже такая задача
                if scheduler.get_job(job_id):
                    scheduler.remove_job(job_id)
                
                # Получаем сессию
                cursor = conn.cursor()
                row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (st["user_id"],)).fetchone()
                if not row:
                    await event.respond("⚠ Не удалось найти сессию для этого аккаунта.")
                    broadcast_solo_state.pop(event.sender_id, None)
                    cursor.close()
                    return
                session_string = row[0]
                cursor.close()
                
                # Устанавливаем триггер для планировщика
                trigger = IntervalTrigger(minutes=st["interval"])
                next_run = datetime.datetime.now() + datetime.timedelta(seconds=10)  # Запускаем через 10 секунд
                
                # Добавляем задачу в планировщик
                scheduler.add_job(
                    send_broadcast_message,
                    trigger,
                    args=[st["user_id"], st["group_id"], st["text"], session_string, photo],
                    id=job_id,
                    next_run_time=next_run,
                    replace_existing=True
                )
                
                # Запускаем планировщик, если он еще не запущен
                if not scheduler.running:
                    scheduler.start()
                
                message_type = "только с фото" if st["step"] == "photo_only" else "с фото"
                await event.respond(f"✅ Запустил: каждые {st['interval']} мин {message_type}.")
                broadcast_solo_state.pop(event.sender_id, None)
            except Exception as e:
                logger.error(f"Ошибка при обработке фото: {e}")
                await event.respond("⚠ Произошла ошибка при обработке фото. Попробуйте еще раз или выберите рассылку без фото.")
        else:
            await event.respond("⚠ Пожалуйста, отправьте фото или выберите рассылку без фото (/start).")
        return


@bot.on(Query(data=lambda d: d.decode() == "photo_yes"))
async def photo_yes_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_solo_state:
        await event.respond("⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_solo_state[user_id]
    st["step"] = "photo"
    
    await event.respond("📤 Пожалуйста, отправьте фото, которое хотите прикрепить к сообщению:")


@bot.on(Query(data=lambda d: d.decode() == "photo_only"))
async def photo_only_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_solo_state:
        await event.respond("⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_solo_state[user_id]
    st["step"] = "photo_only"
    st["text"] = ""  # Пустой текст для отправки только фото
    
    await event.respond("📤 Пожалуйста, отправьте фото, которое хотите отправить без текста:")


@bot.on(Query(data=lambda d: d.decode() == "photo_no"))
async def photo_no_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_solo_state:
        await event.respond("⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_solo_state[user_id]
    
    # Запускаем рассылку без фото
    job_id = f"broadcast_{st['user_id']}_{gid_key(st['group_id'])}"
    
    # Обновляем данные рассылки в базе
    create_broadcast_data(st["user_id"], st["group_id"], st["text"], st["interval"])
    
    # Проверяем, есть ли уже такая задача
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    
    # Получаем сессию
    cursor = conn.cursor()
    row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (st["user_id"],)).fetchone()
    if not row:
        await event.respond("⚠ Не удалось найти сессию для этого аккаунта.")
        broadcast_solo_state.pop(user_id, None)
        cursor.close()
        return
    session_string = row[0]
    cursor.close()
    
    # Устанавливаем триггер для планировщика
    trigger = IntervalTrigger(minutes=st["interval"])
    next_run = datetime.datetime.now() + datetime.timedelta(seconds=10)  # Запускаем через 10 секунд
    
    # Добавляем задачу в планировщик
    scheduler.add_job(
        send_broadcast_message,
        trigger,
        args=[st["user_id"], st["group_id"], st["text"], session_string, None],
        id=job_id,
        next_run_time=next_run,
        replace_existing=True
    )
    
    # Запускаем планировщик, если он еще не запущен
    if not scheduler.running:
        scheduler.start()
    
    await event.respond(f"✅ Запустил: каждые {st['interval']} мин.")
    broadcast_solo_state.pop(user_id, None)


@bot.on(Query(data=lambda data: data.decode().startswith("StartResumeBroadcast_")))
async def start_resume_broadcast(event: callback_query) -> None:
    data = event.data.decode()
    parts = data.split("_")

    if len(parts) < 3:
        await event.respond("⚠ Произошла ошибка при обработке данных. Попробуйте еще раз.")
        return

    try:
        user_id = int(parts[1])
        group_id = int(parts[2])
    except ValueError as e:
        await event.respond(f"⚠ Ошибка при извлечении данных: {e}")
        return
    cursor = conn.cursor()
    job_id = f"broadcast_{user_id}_{gid_key(group_id)}"
    existing_job = scheduler.get_job(job_id)

    if existing_job:
        await event.respond("⚠ Рассылка уже активна для этой группы.")
        cursor.close()
        return

    # Получаем данные рассылки из базы
    cursor.execute("""
                SELECT broadcast_text, interval_minutes, photo_url 
                FROM broadcasts 
                WHERE user_id = ? AND group_id = ?
            """, (user_id, gid_key(group_id)))
    row = cursor.fetchone()

    if not row:
        # Если данных нет, предлагаем настроить рассылку
        await event.respond("⚠ Рассылка еще не настроена для этой группы. Пожалуйста, настройте текст и интервал рассылки.")
        cursor.close()
        return
    
    broadcast_text = row[0]
    interval_minutes = row[1]
    photo_url = row[2] if len(row) > 2 else None
    
    if not broadcast_text or not interval_minutes or interval_minutes <= 0:
        await event.respond("⚠ Пожалуйста, убедитесь, что текст рассылки и корректный интервал установлены.")
        cursor.close()
        return
    
    # Получаем сессию пользователя
    session_string_row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?",
                                        (user_id,)).fetchone()
    if not session_string_row:
        await event.respond("⚠ Ошибка: не найден session_string для аккаунта.")
        cursor.close()
        return
    
    session_string = session_string_row[0]
    
    # Проверяем, существует ли запись о группе
    group_row = cursor.execute("SELECT group_username FROM groups WHERE user_id = ? AND group_id = ?", 
                              (user_id, group_id)).fetchone()
    if not group_row:
        await event.respond(f"⚠ Группа не найдена в базе данных для user_id={user_id}, group_id={group_id}.")
        cursor.close()
        return

    # Активируем рассылку в базе данных
    cursor.execute("""
        UPDATE broadcasts 
        SET is_active = ?, error_reason = NULL
        WHERE user_id = ? AND group_id = ?
    """, (True, user_id, gid_key(group_id)))
    
    # Если запись не существует, создаем новую
    if cursor.rowcount == 0:
        cursor.execute("""
            INSERT INTO broadcasts (user_id, group_id, broadcast_text, interval_minutes, is_active, error_reason, photo_url)
            VALUES (?, ?, ?, ?, ?, NULL, ?)
        """, (user_id, gid_key(group_id), broadcast_text, interval_minutes, True, photo_url))
        
    conn.commit()
    
    # Создаем задачу в планировщике
    trigger = IntervalTrigger(minutes=interval_minutes)
    next_run = datetime.datetime.now() + datetime.timedelta(seconds=10)  # Запускаем через 10 секунд
    
    # Добавляем задачу в планировщик
    scheduler.add_job(
        send_broadcast_message,
        trigger,
        args=[user_id, group_id, broadcast_text, session_string, photo_url],
        id=job_id,
        next_run_time=next_run,
        replace_existing=True
    )
    
    # Запускаем планировщик, если он еще не запущен
    if not scheduler.running:
        scheduler.start()
    
    await event.respond(f"✅ Рассылка успешно запущена! Первое сообщение будет отправлено через 10 секунд, затем каждые {interval_minutes} минут.")
    cursor.close()


@bot.on(Query(data=lambda data: data.decode().startswith("StopAccountBroadcast_")))
async def stop_broadcast(event: callback_query) -> None:
    data = event.data.decode()
    try:
        user_id, group_id = map(int, data.split("_")[1:])
    except ValueError as e:
        await event.respond(f"⚠ Ошибка при извлечении user_id и group_id: {e}")
        return
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
        
        # Пытаемся получить entity группы
        try:
            # Проверяем, является ли username числом (ID группы) или именем пользователя
            if group_username.startswith('@'):
                # Это username группы
                group = await client.get_entity(group_username)
            else:
                # Пробуем получить entity по ID
                try:
                    group_id_int = int(group_username)
                    group = await get_entity_by_id(client, group_id_int)
                    if not group:
                        await event.respond(f"⚠ Ошибка: не удалось получить информацию о группе {group_username}.")
                        await client.disconnect()
                        cursor.close()
                        return
                except ValueError:
                    # Если не можем преобразовать в число, пробуем использовать как есть
                    group = await client.get_entity(group_username)
        except Exception as entity_error:
            logger.error(f"Ошибка при получении entity для группы {group_username}: {entity_error}")
            
            # Пробуем получить entity другим способом
            if "Cannot find any entity corresponding to" in str(entity_error):
                try:
                    # Преобразуем username в ID, если это возможно
                    try:
                        group_id_int = int(group_username)
                        group = await get_entity_by_id(client, group_id_int)
                        if not group:
                            # Если не удалось получить entity, останавливаем задачу без информации о группе
                            job_id = f"broadcast_{user_id}_{gid_key(group_id)}"
                            job = scheduler.get_job(job_id)
                            
                            if job:
                                job.remove()
                                cursor.execute("UPDATE broadcasts SET is_active = ?, error_reason = ? WHERE user_id = ? AND group_id = ?",
                                               (False, "Администратор остановил рассылку", user_id, gid_key(group_id)))
                                conn.commit()
                                await event.respond(f"⛔ Рассылка в группу с ID {group_id} остановлена.")
                                await client.disconnect()
                                cursor.close()
                                return
                    except ValueError:
                        # Если username не является числом, пробуем другие методы
                        return
                    except Exception as alt_error:
                        logger.error(f"[DEBUG] Ошибка при альтернативном получении Entity: {alt_error}")
                        return
                except Exception as alt_error:
                    logger.error(f"[DEBUG] Ошибка при альтернативном получении Entity: {alt_error}")
                    return
            else:
                await event.respond(f"⚠ Ошибка при получении информации о группе: {str(entity_error)}")
                await client.disconnect()
                cursor.close()
                return
        
        # Останавливаем задачу
        job_id = f"broadcast_{user_id}_{gid_key(group_id)}"
        job = scheduler.get_job(job_id)
        
        if job:
            job.remove()
            cursor.execute("UPDATE broadcasts SET is_active = ?, error_reason = ? WHERE user_id = ? AND group_id = ?",
                           (False, "Администратор остановил рассылку", user_id, gid_key(group_id)))
            conn.commit()
            await event.respond(f"⛔ Рассылка в группу **{getattr(group, 'title', group_username)}** остановлена.")
        else:
            await event.respond(f"⚠ Рассылка в группу **{getattr(group, 'title', group_username)}** не была запущена.")
    except Exception as e:
        logger.error(f"Ошибка при остановке рассылки: {e}")
        
        # Если произошла неожиданная ошибка, все равно пытаемся остановить задачу
        try:
            job_id = f"broadcast_{user_id}_{gid_key(group_id)}"
            job = scheduler.get_job(job_id)
            
            if job:
                job.remove()
                cursor.execute("UPDATE broadcasts SET is_active = ?, error_reason = ? WHERE user_id = ? AND group_id = ?",
                               (False, "Администратор остановил рассылку", user_id, gid_key(group_id)))
                conn.commit()
                await event.respond(f"⛔ Рассылка в группу с ID {group_id} остановлена (с ошибкой: {str(e)}).")
            else:
                await event.respond(f"⚠ Рассылка в группу с ID {group_id} не была запущена.")
        except Exception as stop_error:
            await event.respond(f"⚠ Критическая ошибка при остановке рассылки: {str(stop_error)}")
    finally:
        await client.disconnect()
        cursor.close()
