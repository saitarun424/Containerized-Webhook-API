import sqlite3

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        message_id TEXT PRIMARY KEY,
        from_msisdn TEXT NOT NULL,
        to_msisdn TEXT NOT NULL,
        ts TEXT NOT NULL,
        text TEXT,
        created_at TEXT NOT NULL
    );
    """)
    conn.commit()
    conn.close()