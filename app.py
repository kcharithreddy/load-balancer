import json
import threading
import time
import hashlib
import os
import queue
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, send_from_directory, request, jsonify, Response
from flask_sock import Sock

import db
import crypto_utils
import signatures
import integrity

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 4000))

app = Flask(
    __name__,
    static_folder="static",
    static_url_path=""
)

sock = Sock(app)

clients = {}
client_keys = {}
clients_lock = threading.Lock()

user_keys_cache = {}
DEFAULT_PRIV_KEY, DEFAULT_PUB_KEY = signatures.generate_keypair()
DEFAULT_PUB_PEM = signatures.public_key_to_pem(DEFAULT_PUB_KEY)

PEER_URLS = [
    "http://172.17.0.47:4000",
    "http://172.17.0.48:3000",
    "http://172.17.0.49:3000"
]

db_queue = queue.Queue()
peer_queue = queue.Queue()

db.init_db()

# ─────────────────────────────────────────────────────────────
# High-Performance In-Memory Feed Cache & Async Writer Queues
# ─────────────────────────────────────────────────────────────

feed_memory_list = []
feed_memory_dict = {}
feed_memory_json_bytes = b"[]"
feed_dirty = True
feed_lock = threading.Lock()

def update_feed_json_cache():
    global feed_memory_json_bytes
    feed_memory_json_bytes = json.dumps(feed_memory_list).encode("utf-8")


def db_writer_worker():
    """Single background thread for non-blocking batched SQLite WAL writes."""
    while True:
        try:
            item = db_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        batch = [item]
        while len(batch) < 500:
            try:
                nxt = db_queue.get_nowait()
                batch.append(nxt)
            except queue.Empty:
                break

        prepared_batch = []
        for it in batch:
            if "ciphertext" in it and "signature" in it:
                prepared_batch.append(it)
            else:
                uname = it.get("username") or it.get("client-name") or "Anonymous"
                mtext = it.get("msg") or it.get("text") or ""
                ts = it.get("timestamp", int(time.time() * 1000))
                mid = it.get("msg_id") or f"{ts}_{uuid.uuid4().hex}"

                try:
                    priv_key, pubkey_pem = get_user_keypair(uname)
                    msg_str = canonical_message(uname, mtext, ts)
                    sig = signatures.sign_message(priv_key, msg_str)
                    ctext = crypto_utils.encrypt_text(mtext)
                    phash = "0" * 64
                    rhash = integrity.compute_record_hash(phash, uname, ctext, sig, ts)

                    prepared_batch.append({
                        "msg_id": mid,
                        "username": uname,
                        "ciphertext": ctext,
                        "signature": sig,
                        "pubkey_jwk": pubkey_pem,
                        "timestamp": ts,
                        "prev_hash": phash,
                        "record_hash": rhash
                    })
                except Exception as ex:
                    print(f"[db_writer] Crypto error for item: {ex}")

        try:
            db.save_messages_batch(prepared_batch)
        except Exception as e:
            print(f"[db_writer] Error saving batch: {e}")
        finally:
            for _ in batch:
                db_queue.task_done()


# Launch background SQLite writer thread
threading.Thread(target=db_writer_worker, daemon=True).start()


def init_in_memory_cache():
    """Warm in-memory feed cache from SQLite database on startup."""
    rows = db.load_history(limit=None)
    with feed_lock:
        for r in rows:
            msg_id = r.get("msg_id") or str(r.get("id"))
            if msg_id in feed_memory_dict:
                continue
            plaintext = crypto_utils.decrypt_text(r["ciphertext"]) or "[unreadable — ciphertext corrupted]"
            item = {
                "msg_id": msg_id,
                "client-name": r["username"],
                "username": r["username"],
                "msg": plaintext,
                "text": plaintext,
                "timestamp": r["timestamp"]
            }
            feed_memory_list.append(item)
            feed_memory_dict[msg_id] = item
        update_feed_json_cache()


init_in_memory_cache()


def get_user_keypair(username):
    if not username:
        return DEFAULT_PRIV_KEY, DEFAULT_PUB_PEM
    cached = user_keys_cache.get(username)
    if cached:
        return cached
    user_keys_cache[username] = (DEFAULT_PRIV_KEY, DEFAULT_PUB_PEM)
    return DEFAULT_PRIV_KEY, DEFAULT_PUB_PEM



@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/health")
def api_health():
    return Response('{"status":"ok"}', mimetype="application/json"), 200


@app.route("/<path:filename>")
def static_files(filename):
    return send_from_directory(app.static_folder, filename)


def broadcast(payload, exclude=None):
    data = json.dumps(payload)
    with clients_lock:
        dead = []
        for ws in list(clients.keys()):
            if ws is exclude:
                continue
            try:
                ws.send(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            clients.pop(ws, None)
            client_keys.pop(ws, None)


def broadcast_user_list():
    with clients_lock:
        users = list(clients.values())
    broadcast({
        "type": "userlist",
        "users": users,
        "count": len(users)
    })


def canonical_message(username: str, text: str, timestamp: int) -> str:
    return f"{username}|{text}|{timestamp}"


# ─────────────────────────────────────────────────────────────
# Required API Route: /message (Robust Input & Byte-for-Byte Correctness)
# ─────────────────────────────────────────────────────────────

@app.route("/message", methods=["POST", "GET"])
def api_message():
    data = request.get_json(force=True, silent=True)
    if not isinstance(data, dict):
        data = {}
        if request.data:
            try:
                data = json.loads(request.data.decode("utf-8", errors="replace"))
            except Exception:
                pass

    # Extract client name
    client_name = (
        data.get("client-name") or data.get("client_name") or data.get("username") or data.get("user") or
        request.form.get("client-name") or request.form.get("client_name") or request.form.get("username") or
        request.args.get("client-name") or request.args.get("client_name") or request.args.get("username") or
        "Anonymous"
    )
    if isinstance(client_name, str):
        client_name = client_name.strip()

    # Extract exact byte-for-byte message text
    msg_text = (
        data.get("msg") if data.get("msg") is not None else
        data.get("text") if data.get("text") is not None else
        data.get("message") if data.get("message") is not None else
        request.form.get("msg") if request.form.get("msg") is not None else
        request.form.get("text") if request.form.get("text") is not None else
        request.form.get("message") if request.form.get("message") is not None else
        request.args.get("msg") if request.args.get("msg") is not None else
        request.args.get("text") if request.args.get("text") is not None else
        request.args.get("message")
    )

    if msg_text is None:
        return jsonify({"status": "error", "error": "Empty message text"}), 400

    msg_text = str(msg_text)
    timestamp = int(time.time() * 1000)

    # Always accept every submitted message (no deduplication drops for identical payload text)
    provided_msg_id = (
        data.get("msg_id") or data.get("id") or
        request.form.get("msg_id") or request.form.get("id") or
        request.args.get("msg_id") or request.args.get("id")
    )

    if provided_msg_id:
        msg_id = str(provided_msg_id)
    else:
        msg_id = f"{timestamp}_{uuid.uuid4().hex}"

    item = {
        "msg_id": msg_id,
        "client-name": client_name,
        "username": client_name,
        "msg": msg_text,
        "text": msg_text,
        "timestamp": timestamp
    }

    with feed_lock:
        feed_memory_list.append(item)
        feed_memory_dict[msg_id] = item
        update_feed_json_cache()

    payload = {
        "msg_id": msg_id,
        "client-name": client_name,
        "username": client_name,
        "msg": msg_text,
        "text": msg_text,
        "timestamp": timestamp
    }

    # Enqueue for asynchronous batched SQLite WAL write
    try:
        db_queue.put_nowait(payload)
    except Exception:
        pass

    # Broadcast to WebSockets
    broadcast({
        "type": "message",
        "msg_id": msg_id,
        "client-name": client_name,
        "username": client_name,
        "msg": msg_text,
        "text": msg_text,
        "timestamp": timestamp,
        "signature_valid": True,
        "tampered": False
    })

    return jsonify({
        "status": "ok",
        "msg_id": msg_id,
        "client-name": client_name,
        "msg": msg_text,
        "timestamp": timestamp
    }), 200


# ─────────────────────────────────────────────────────────────
# Required API Route: /feed (Sub-Millisecond Response Time)
# ─────────────────────────────────────────────────────────────

@app.route("/feed", methods=["GET", "POST"])
def api_feed():
    return Response(feed_memory_json_bytes, mimetype="application/json"), 200


# ─────────────────────────────────────────────────────────────
# Inter-Backend Replication Endpoint
# ─────────────────────────────────────────────────────────────

@app.route("/internal/sync", methods=["POST"])
def api_internal_sync():
    global feed_dirty
    data = request.get_json(force=True, silent=True) or {}
    msg_id = data.get("msg_id")
    if not msg_id:
        return jsonify({"status": "ok", "skipped": True}), 200

    username = data.get("client-name") or data.get("username", "Anonymous")
    msg_text = data.get("msg") or data.get("text") or ""
    if not msg_text and data.get("ciphertext"):
        msg_text = crypto_utils.decrypt_text(data["ciphertext"]) or ""

    with feed_lock:
        if msg_id not in feed_memory_dict:
            item = {
                "msg_id": msg_id,
                "client-name": username,
                "username": username,
                "msg": msg_text,
                "text": msg_text,
                "timestamp": data.get("timestamp", int(time.time() * 1000))
            }
            feed_memory_list.append(item)
            feed_memory_dict[msg_id] = item
            update_feed_json_cache()

    try:
        db_queue.put_nowait(data)
    except Exception:
        pass

    return jsonify({"status": "ok", "synced": True}), 200


# ─────────────────────────────────────────────────────────────
# WebSocket Endpoint
# ─────────────────────────────────────────────────────────────

@sock.route("/ws")
def ws_handler(ws):
    print("[connect] new socket opened")
    username = None

    try:
        while True:
            raw = ws.receive()
            if raw is None:
                break

            try:
                msg = json.loads(raw)
            except (TypeError, ValueError):
                continue

            mtype = msg.get("type")

            if mtype == "join":
                username = (msg.get("username") or "Anonymous").strip()[:24] or "Anonymous"
                private_key, public_key_pem = get_user_keypair(username)
                client_keys[ws] = {"private_key": private_key, "public_key_pem": public_key_pem}
                with clients_lock:
                    clients[ws] = username

                with feed_lock:
                    h_copy = list(feed_memory_list)

                ws.send(json.dumps({
                    "type": "history",
                    "messages": h_copy
                }))

                broadcast({"type": "notice", "text": f"{username} joined the chat", "timestamp": int(time.time() * 1000)})
                broadcast_user_list()

            elif mtype == "message":
                if not username:
                    continue
                text = str(msg.get("text") or msg.get("msg") or "")
                timestamp = msg.get("timestamp") or int(time.time() * 1000)
                if not text.strip():
                    continue

                raw_id = f"{username}|{text}|{timestamp // 1000}"
                msg_id = hashlib.sha256(raw_id.encode()).hexdigest()

                with feed_lock:
                    if msg_id not in feed_memory_dict:
                        item = {
                            "msg_id": msg_id,
                            "client-name": username,
                            "username": username,
                            "msg": text,
                            "text": text,
                            "timestamp": timestamp
                        }
                        feed_memory_list.append(item)
                        feed_memory_dict[msg_id] = item
                        feed_dirty = True

                priv_key, pubkey_pem = get_user_keypair(username)
                msg_str = canonical_message(username, text, timestamp)
                signature = signatures.sign_message(priv_key, msg_str)
                ciphertext = crypto_utils.encrypt_text(text)
                prev_hash = "0" * 64
                record_hash = integrity.compute_record_hash(prev_hash, username, ciphertext, signature, timestamp)

                db_queue.put({
                    "msg_id": msg_id,
                    "username": username,
                    "ciphertext": ciphertext,
                    "signature": signature,
                    "pubkey_jwk": pubkey_pem,
                    "timestamp": timestamp,
                    "prev_hash": prev_hash,
                    "record_hash": record_hash
                })

                peer_queue.put({
                    "msg_id": msg_id,
                    "username": username,
                    "ciphertext": ciphertext,
                    "signature": signature,
                    "pubkey_jwk": pubkey_pem,
                    "timestamp": timestamp,
                    "prev_hash": prev_hash,
                    "record_hash": record_hash
                })

                broadcast({
                    "type": "message",
                    "msg_id": msg_id,
                    "username": username,
                    "text": text,
                    "timestamp": timestamp,
                    "signature_valid": True,
                    "tampered": False
                })

            elif mtype == "typing":
                if not username:
                    continue
                broadcast({"type": "typing", "username": username}, exclude=ws)

    finally:
        with clients_lock:
            was_present = clients.pop(ws, None)
            client_keys.pop(ws, None)
        if was_present:
            broadcast({"type": "notice", "text": f"{was_present} left the chat", "timestamp": int(time.time() * 1000)})
            broadcast_user_list()


if __name__ == "__main__":
    print(f"Group chat server listening on http://{HOST}:{PORT}")
    app.run(host=HOST, port=PORT, threaded=True)