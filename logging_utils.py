import logging
import json
from datetime import datetime

def log_event(**kwargs):
    log = {
        "ts": datetime.utcnow().isoformat() + "Z",
        **kwargs
    }
    print(json.dumps(log))