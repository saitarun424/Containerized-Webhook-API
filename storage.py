import sqlite3
from datetime import datetime
def get_conn(db_path):
    return sqlite3.connect(db_path)
def insert_message(db_path, msg):
    conn = get_conn(db_path)
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?)""",
            (
                msg["message_id"],
                msg["from"],
                msg["to"],
                msg["ts"],
                msg.get("text"),
                datetime.utcnow().isoformat() + "Z"
            )
        )
        conn.commit()
        return "created"
    except sqlite3.IntegrityError:
        return "duplicate"
    finally:
        conn.close()

def list_messages(db_path, limit, offset, from_filter=None, since=None, q=None):
    conn = get_conn(db_path)
    cur = conn.cursor()

    query = """
        SELECT message_id, from_msisdn, to_msisdn, ts, text
        FROM messages
        WHERE 1=1
    """
    params = []

    if from_filter:
        query += " AND from_msisdn = ?"
        params.append(from_filter)

    if since:
        query += " AND ts >= ?"
        params.append(since)

    if q:
        query += " AND LOWER(text) LIKE ?"
        params.append(f"%{q.lower()}%")

    query += " ORDER BY ts ASC, message_id ASC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    return rows


def count_messages(db_path):
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM messages")
    total = cur.fetchone()[0]
    conn.close()
    return total
def count_messages_filtered(db_path, from_filter=None, since=None, q=None):
    conn = get_conn(db_path)
    cur = conn.cursor()

    query = "SELECT COUNT(*) FROM messages WHERE 1=1"
    params = []

    if from_filter:
        query += " AND from_msisdn = ?"
        params.append(from_filter)

    if since:
        query += " AND ts >= ?"
        params.append(since)

    if q:
        query += " AND LOWER(text) LIKE ?"
        params.append(f"%{q.lower()}%")

    cur.execute(query, params)
    total = cur.fetchone()[0]
    conn.close()
    return total

def get_stats(db_path):
    conn = get_conn(db_path)
    cur = conn.cursor()

    # Total messages
    cur.execute("SELECT COUNT(*) FROM messages")
    total_messages = cur.fetchone()[0]

    # Unique senders
    cur.execute("SELECT COUNT(DISTINCT from_msisdn) FROM messages")
    senders_count = cur.fetchone()[0]

    # Messages per sender
    cur.execute("""
        SELECT from_msisdn, COUNT(*)
        FROM messages
        GROUP BY from_msisdn
        ORDER BY from_msisdn
    """)
    messages_per_sender = [
        {"from": r[0], "count": r[1]}
        for r in cur.fetchall()
    ]

    # First and last message timestamps
    cur.execute("SELECT MIN(ts), MAX(ts) FROM messages")
    first_ts, last_ts = cur.fetchone()

    conn.close()

    return {
        "total_messages": total_messages,
        "senders_count": senders_count,
        "messages_per_sender": messages_per_sender,
        "first_message_ts": first_ts,
        "last_message_ts": last_ts
    }
