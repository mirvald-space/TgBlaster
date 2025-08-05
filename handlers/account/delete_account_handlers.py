from loguru import logger

from config import Query, bot, conn, callback_query


@bot.on(Query(data=lambda event: event.decode().startswith(f"delete_account_")))
async def handle_user_input(event: callback_query):
    user_id: int = int(event.data.decode().strip().split("_")[2])
    cursor = conn.cursor()
    cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,))
    user = cursor.fetchone()

    if user:
        cursor.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        cursor.execute("""DELETE FROM groups WHERE user_id = ?""", (user_id, ))
        conn.commit()
        logger.info(f"✅ Аккаунт  id={user_id} успешно удален.")
        await event.respond(f"✅ Аккаунт id={user_id} успешно удален.")
    else:
        logger.warning(f"Аккаунт id={user} не найден")
        await event.respond("⚠ Этот аккаунт не найден в базе данных.")
    cursor.close()
