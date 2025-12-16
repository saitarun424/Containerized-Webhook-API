import os

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////data/app.db")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

if not WEBHOOK_SECRET:
    raise RuntimeError("WEBHOOK_SECRET is not set")