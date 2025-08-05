from config import conn


def create_table() -> None:
    """
    Создает таблицы SQL если их нет
    :return:
        None
    """
    start_cursor = conn.cursor()
    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS pre_groups (
            group_id INTEGER PRIMARY KEY AUTOINCREMENT, 
            group_username TEXT UNIQUE)""")

    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            group_id INTEGER,
            group_username TEXT,
            user_id INTEGER)""")

    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            user_id INTEGER PRIMARY KEY,
            session_string TEXT)""")

    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS broadcasts ( 
            user_id INTEGER, 
            group_id INTEGER, 
            session_string TEXT, 
            broadcast_text TEXT, 
            interval_minutes INTEGER,
            is_active BOOLEAN,
            error_reason TEXT,
            photo_url TEXT)""")
    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS send_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            group_id INTEGER,
            group_name TEXT,
            sent_at TEXT,
            message_text TEXT);""")

    # Миграция: добавляем столбец error_reason, если он не существует
    try:
        start_cursor.execute("ALTER TABLE broadcasts ADD COLUMN error_reason TEXT")
        conn.commit()
    except:
        pass

    # Миграция: добавляем столбец photo_url, если он не существует
    try:
        start_cursor.execute("ALTER TABLE broadcasts ADD COLUMN photo_url TEXT")
        conn.commit()
    except:
        pass

    conn.commit()
    start_cursor.close()


def delete_table() -> None:
    """
    После остановки бота меняет статус активных рассылок на неактивный,
    сохраняя при этом тексты и интервалы рассылок
    :return:
        None
    """
    end_cursor = conn.cursor()
    # Вместо удаления всех записей, просто меняем статус на неактивный
    end_cursor.execute("""UPDATE broadcasts SET is_active = ? WHERE is_active = ?""", (False, True))
    conn.commit()
    end_cursor.close()
