import sqlite3, os

DB_PATH = os.path.join(os.path.dirname(__file__), "businesses.db")

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS businesses (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id       TEXT,
            name             TEXT,
            category         TEXT,
            address          TEXT,
            city             TEXT,
            state            TEXT,
            country          TEXT,
            latitude         REAL,
            longitude        REAL,
            phone            TEXT,
            website          TEXT,
            rating           TEXT,
            review_count     TEXT,
            opening_hours    TEXT,
            price_level      TEXT,
            business_status  TEXT,
            maps_url         TEXT,
            date_scraped     TEXT,
            UNIQUE(session_id, name, latitude, longitude)
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_session ON businesses(session_id)")
    # Migrate older DBs
    for col, typedef in [
        ("rating",          "TEXT"),
        ("review_count",    "TEXT"),
        ("price_level",     "TEXT"),
        ("business_status", "TEXT"),
        ("session_id",      "TEXT"),
    ]:
        try:
            c.execute(f"ALTER TABLE businesses ADD COLUMN {col} {typedef}")
        except Exception:
            pass
    conn.commit()
    conn.close()

def insert_businesses_batch(records):
    if not records:
        return 0
    conn = get_conn()
    c = conn.cursor()
    try:
        rows = [(
            r.get("session_id",""),   r.get("name"),          r.get("category"),
            r.get("address"),         r.get("city"),           r.get("state"),
            r.get("country"),         r.get("latitude"),       r.get("longitude"),
            r.get("phone",""),        r.get("website",""),     r.get("rating",""),
            r.get("review_count",""), r.get("opening_hours",""),r.get("price_level",""),
            r.get("business_status",""), r.get("maps_url"),    r.get("date_scraped"),
        ) for r in records]
        c.executemany("""
            INSERT OR IGNORE INTO businesses
            (session_id, name, category, address, city, state, country,
             latitude, longitude, phone, website, rating, review_count,
             opening_hours, price_level, business_status, maps_url, date_scraped)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
        conn.commit()
        return c.rowcount
    except Exception as e:
        print(f"Batch insert error: {e}")
        conn.rollback()
        return 0
    finally:
        conn.close()

def insert_business(record):
    return insert_businesses_batch([record]) > 0

def get_businesses_by_session(session_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT name, category, address, city, state, country,
               latitude, longitude, phone, website, rating, review_count,
               opening_hours, price_level, business_status, maps_url, date_scraped
        FROM businesses WHERE session_id = ?
    """, (session_id,))
    rows = c.fetchall()
    conn.close()
    cols = ["name","category","address","city","state","country",
            "latitude","longitude","phone","website","rating","review_count",
            "opening_hours","price_level","business_status","maps_url","date_scraped"]
    return [dict(zip(cols, r)) for r in rows]

def get_all_businesses():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT name, category, address, city, state, country,
               latitude, longitude, phone, website, rating, review_count,
               opening_hours, price_level, business_status, maps_url, date_scraped
        FROM businesses
    """)
    rows = c.fetchall()
    conn.close()
    cols = ["name","category","address","city","state","country",
            "latitude","longitude","phone","website","rating","review_count",
            "opening_hours","price_level","business_status","maps_url","date_scraped"]
    return [dict(zip(cols, r)) for r in rows]

def get_stats():
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM businesses")
    total = c.fetchone()[0]
    conn.close()
    return {"total": total}
