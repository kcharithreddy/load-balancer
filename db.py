import sqlite3
import threading
import uuid
import hashlib

DB_PATH = "chat.db"
_lock = threading.Lock()


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30.0)
    conn.row_factory = sqlite3.Row
    # Enable WAL mode, memory caching, and busy timeout for high concurrency read/write
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA cache_size=-64000;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_id      TEXT UNIQUE,
                username    TEXT NOT NULL,
                ciphertext  TEXT NOT NULL,
                signature   TEXT NOT NULL,
                pubkey_jwk  TEXT NOT NULL,
                timestamp   INTEGER NOT NULL,
                prev_hash   TEXT NOT NULL,
                record_hash TEXT NOT NULL
            )
        """)
        # Safely add msg_id column if table existed without it
        try:
            conn.execute("ALTER TABLE messages ADD COLUMN msg_id TEXT;")
        except Exception:
            pass
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                username   TEXT PRIMARY KEY,
                pubkey_jwk TEXT NOT NULL
            )
        """)
        # Create index on msg_id for instant deduplication lookup
        conn.execute("CREATE INDEX IF NOT EXISTS idx_msg_id ON messages(msg_id);")
        conn.commit()


def get_last_hash() -> str:
    """Tail of the hash chain — needed so the next message can link to it."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT record_hash FROM messages ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return row["record_hash"] if row else "0" * 64


def has_msg_id(msg_id: str) -> bool:
    if not msg_id:
        return False
    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM messages WHERE msg_id = ? LIMIT 1", (msg_id,)
        ).fetchone()
        return row is not None


def save_message(username, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash, msg_id=None):
    if not msg_id:
        raw = f"{username}|{ciphertext}|{timestamp}"
        msg_id = hashlib.sha256(raw.encode()).hexdigest()

    with get_conn() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO messages
               (msg_id, username, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (msg_id, username, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash)
        )
        conn.commit()
    return msg_id


def save_messages_batch(batch):
    if not batch:
        return
    with get_conn() as conn:
        conn.executemany(
            """INSERT OR IGNORE INTO messages
               (msg_id, username, ciphertext, signature, pubkey_jwk, timestamp, prev_hash, record_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [(item["msg_id"], item["username"], item["ciphertext"], item["signature"], item["pubkey_jwk"], item["timestamp"], item["prev_hash"], item["record_hash"]) for item in batch]
        )
        conn.commit()


def load_history(limit=None):
    with get_conn() as conn:
        if limit:
            rows = conn.execute(
                "SELECT * FROM messages ORDER BY id ASC LIMIT ?", (limit,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM messages ORDER BY id ASC"
            ).fetchall()
        return [dict(r) for r in rows]


def upsert_user_pubkey(username, pubkey_jwk_str):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO users (username, pubkey_jwk) VALUES (?, ?) "
            "ON CONFLICT(username) DO UPDATE SET pubkey_jwk = excluded.pubkey_jwk",
            (username, pubkey_jwk_str)
        )
        conn.commit()


def get_user_pubkey(username):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT pubkey_jwk FROM users WHERE username = ?", (username,)
        ).fetchone()
        return row["pubkey_jwk"] if row else None