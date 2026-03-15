import json
import os

PROGRESS_FILE = os.path.join(os.path.dirname(__file__), "progress.json")

DEFAULT = {
    "session_id": "",
    "business_type": "",
    "city": "",
    "state": "",
    "country": "",
    "status": "idle",
    "fetched": 0,
    "inserted": 0,
    "message": "",
    "error": ""
}

def get_progress():
    if not os.path.exists(PROGRESS_FILE):
        return dict(DEFAULT)
    try:
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    except Exception:
        return dict(DEFAULT)

def update_progress(**kwargs):
    prog = get_progress()
    prog.update(kwargs)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(prog, f)

def reset_progress():
    with open(PROGRESS_FILE, "w") as f:
        json.dump(dict(DEFAULT), f)
