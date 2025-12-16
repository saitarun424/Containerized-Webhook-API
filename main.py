from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel, Field
import hmac, hashlib
from app.config import DATABASE_URL, WEBHOOK_SECRET
from app.models import init_db
from app.storage import (
    insert_message,
    list_messages,
    count_messages_filtered,
    get_stats
)

from app.logging_utils import log_event

DB_PATH = DATABASE_URL.replace("sqlite:///", "")
init_db(DB_PATH)

app = FastAPI()

class Message(BaseModel):
    message_id: str = Field(..., min_length=1)
    from_: str = Field(..., alias="from")
    to: str
    ts: str
    text: str | None = None

def verify_signature(secret, body, signature):
    computed = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(computed, signature)

@app.post("/webhook")
async def webhook(request: Request):
    body = await request.body()
    sig = request.headers.get("X-Signature")

    if not sig or not verify_signature(WEBHOOK_SECRET, body, sig):
        log_event(level="ERROR", path="/webhook", result="invalid_signature")
        raise HTTPException(status_code=401, detail="invalid signature")

    data = await request.json()
    result = insert_message(DB_PATH, data)
    log_event(
        level="INFO",
        path="/webhook",
        result=result,
        message_id=data["message_id"]
    )
    return {"status": "ok"}

@app.get("/messages")
def messages(
    limit: int = 50,
    offset: int = 0,
    from_: str | None = None,
    since: str | None = None,
    q: str | None = None
):
    rows = list_messages(
        DB_PATH,
        limit,
        offset,
        from_filter=from_,
        since=since,
        q=q
    )

    total = count_messages_filtered(
        DB_PATH,
        from_filter=from_,
        since=since,
        q=q
    )

    return {
        "data": [
            {
                "message_id": r[0],
                "from": r[1],
                "to": r[2],
                "ts": r[3],
                "text": r[4]
            } for r in rows
        ],
        "total": total,
        "limit": limit,
        "offset": offset
    }

@app.get("/stats")
@app.get("/stats")
def stats():
    return get_stats(DB_PATH)


@app.get("/health/live")
def live():
    return {"status": "live"}

@app.get("/health/ready")
def ready():
    return {"status": "ready"}
