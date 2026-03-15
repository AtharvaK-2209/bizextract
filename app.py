from flask import Flask, render_template, request, jsonify, send_file
import threading, os, uuid
from scraper import run_scraper, resume_scraper
from database import init_db, get_stats
from exporter import export_to_excel
from progress import get_progress, reset_progress

app = Flask(__name__)
init_db()
scraper_thread = None

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/start", methods=["POST"])
def start():
    global scraper_thread
    data          = request.json
    business_type = data.get("business_type", "").strip()
    city          = data.get("city", "").strip()
    state         = data.get("state", "").strip()
    country       = data.get("country", "").strip()
    if not all([business_type, city, country]):
        return jsonify({"error": "Business type, city and country are required."}), 400
    session_id = str(uuid.uuid4())
    reset_progress()
    scraper_thread = threading.Thread(
        target=run_scraper,
        args=(business_type, city, state, country, session_id),
        daemon=True
    )
    scraper_thread.start()
    return jsonify({"status": "started", "session_id": session_id})

@app.route("/resume", methods=["POST"])
def resume():
    global scraper_thread
    prog = get_progress()
    if not prog.get("business_type"):
        return jsonify({"error": "No previous scraping session found."}), 400
    scraper_thread = threading.Thread(target=resume_scraper, daemon=True)
    scraper_thread.start()
    return jsonify({"status": "resumed"})

@app.route("/progress")
def progress():
    prog  = get_progress()
    stats = get_stats()
    prog["total_in_db"] = stats["total"]
    return jsonify(prog)

@app.route("/export")
def export():
    session_id    = request.args.get("session_id", "")
    prog          = get_progress()
    sid           = session_id or prog.get("session_id", "")
    business_type = prog.get("business_type", "businesses")
    city          = prog.get("city", "")
    path = export_to_excel(sid, business_type, city)
    if path and os.path.exists(path):
        safe_name = f"{business_type}_{city}_export.xlsx".replace(" ", "_").lower()
        return send_file(path, as_attachment=True, download_name=safe_name)
    return jsonify({"error": "No data found for this session."}), 400

if __name__ == "__main__":
    app.run(debug=True, port=5000)
