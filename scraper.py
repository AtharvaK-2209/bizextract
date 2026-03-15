"""
scraper.py — OpenStreetMap / Overpass API (100% free, no key needed)
Gets: name, address, phone, website, opening hours, cuisine, category, coords
"""

import requests, os, csv, time, concurrent.futures
from datetime import datetime, timezone
from database import insert_businesses_batch
from progress import update_progress, get_progress
from config import RADIUS_M

CHUNKS_DIR    = os.path.join(os.path.dirname(__file__), "chunks")
os.makedirs(CHUNKS_DIR, exist_ok=True)

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
OVERPASS_URL  = "https://overpass-api.de/api/interpreter"
HEADERS       = {"User-Agent": "BizExtract/1.0 (open source project)"}

# Maps common business type inputs → OSM tags
# Format: list of (key, value) pairs to search
CATEGORY_MAP = {
    "cafe":           [("amenity","cafe")],
    "coffee shop":    [("amenity","cafe")],
    "coffee":         [("amenity","cafe")],
    "restaurant":     [("amenity","restaurant")],
    "hotel":          [("tourism","hotel"),("tourism","guest_house"),("tourism","hostel")],
    "hospital":       [("amenity","hospital"),("amenity","clinic")],
    "pharmacy":       [("amenity","pharmacy")],
    "chemist":        [("amenity","pharmacy"),("shop","chemist")],
    "bank":           [("amenity","bank")],
    "atm":            [("amenity","atm")],
    "school":         [("amenity","school")],
    "college":        [("amenity","college")],
    "university":     [("amenity","university")],
    "gym":            [("leisure","fitness_centre"),("amenity","gym")],
    "fitness":        [("leisure","fitness_centre")],
    "supermarket":    [("shop","supermarket")],
    "grocery":        [("shop","supermarket"),("shop","grocery")],
    "bakery":         [("shop","bakery")],
    "bar":            [("amenity","bar"),("amenity","pub")],
    "pub":            [("amenity","pub"),("amenity","bar")],
    "fast food":      [("amenity","fast_food")],
    "food":           [("amenity","restaurant"),("amenity","fast_food"),("amenity","cafe")],
    "park":           [("leisure","park")],
    "cinema":         [("amenity","cinema")],
    "library":        [("amenity","library")],
    "petrol station": [("amenity","fuel")],
    "gas station":    [("amenity","fuel")],
    "petrol":         [("amenity","fuel")],
    "fuel":           [("amenity","fuel")],
    "clothes":        [("shop","clothes")],
    "clothing":       [("shop","clothes")],
    "mall":           [("shop","mall"),("shop","department_store")],
    "beauty salon":   [("shop","beauty"),("shop","hairdresser")],
    "salon":          [("shop","beauty"),("shop","hairdresser")],
    "dentist":        [("amenity","dentist")],
    "doctor":         [("amenity","doctors")],
    "clinic":         [("amenity","clinic"),("amenity","doctors")],
    "police":         [("amenity","police")],
    "post office":    [("amenity","post_office")],
    "electronics":    [("shop","electronics")],
    "mobile shop":    [("shop","mobile_phone")],
    "mobile":         [("shop","mobile_phone")],
    "jewellery":      [("shop","jewellery"),("shop","jewelry")],
    "jewelry":        [("shop","jewellery"),("shop","jewelry")],
    "bookshop":       [("shop","books")],
    "books":          [("shop","books")],
    "hardware":       [("shop","hardware")],
    "sports":         [("shop","sports"),("leisure","sports_centre")],
    "sport":          [("shop","sports"),("leisure","sports_centre")],
    "yoga":           [("leisure","fitness_centre"),("sport","yoga")],
    "temple":         [("amenity","place_of_worship"),("religion","hindu")],
    "mosque":         [("amenity","place_of_worship"),("religion","muslim")],
    "church":         [("amenity","place_of_worship"),("religion","christian")],
    "hotel":          [("tourism","hotel")],
    "resort":         [("tourism","resort"),("tourism","hotel")],
    "spa":            [("leisure","spa"),("shop","beauty")],
    "laundry":        [("shop","laundry"),("shop","dry_cleaning")],
    "veterinary":     [("amenity","veterinary")],
    "vet":            [("amenity","veterinary")],
    "optician":       [("shop","optician")],
}


# ── 1. Geocode ────────────────────────────────────────────────────────────────
def geocode(city, state, country):
    q = f"{city}, {state}, {country}" if state else f"{city}, {country}"
    try:
        r = requests.get(NOMINATIM_URL,
                         params={"q": q, "format": "json", "limit": 1},
                         headers=HEADERS, timeout=10)
        res = r.json()
        if res:
            return float(res[0]["lat"]), float(res[0]["lon"]), res[0].get("display_name", q)
    except Exception as e:
        print(f"Geocode error: {e}")
    return None, None, None


# ── 2. Build Overpass query ───────────────────────────────────────────────────
def get_tag_pairs(business_type):
    bt = business_type.lower().strip()
    if bt in CATEGORY_MAP:
        return CATEGORY_MAP[bt]
    # Generic fallback — try both amenity and shop
    slug = bt.replace(" ", "_")
    return [("amenity", slug), ("shop", slug), ("leisure", slug)]

def build_query(lat, lon, tag_pairs):
    parts = []
    for key, val in tag_pairs:
        parts.append(f'  nwr["{key}"="{val}"](around:{RADIUS_M},{lat},{lon});')
    return (
        f'[out:json][timeout:60];\n(\n' +
        '\n'.join(parts) +
        f'\n);\nout center tags qt;'
    )


# ── 3. Parse OSM elements ─────────────────────────────────────────────────────
def parse_elements(elements, category, city, state, country, session_id):
    records = []
    seen    = set()
    now     = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    for el in elements:
        tags = el.get("tags", {})

        name = (tags.get("name") or tags.get("name:en") or
                tags.get("brand") or tags.get("operator"))
        if not name:
            continue

        lat = el.get("lat") or el.get("center", {}).get("lat")
        lon = el.get("lon") or el.get("center", {}).get("lon")
        if not lat or not lon:
            continue

        key = (name.lower().strip(), round(float(lat), 5), round(float(lon), 5))
        if key in seen:
            continue
        seen.add(key)

        # Address
        addr_parts = [tags.get(f,"") for f in
                      ("addr:housenumber","addr:street","addr:suburb",
                       "addr:neighbourhood","addr:postcode")]
        address = ", ".join(p for p in addr_parts if p) or tags.get("addr:full","")

        # Phone — OSM has multiple phone tag formats
        phone = (tags.get("phone") or tags.get("contact:phone") or
                 tags.get("telephone") or tags.get("mobile") or "")

        # Website
        website = (tags.get("website") or tags.get("contact:website") or
                   tags.get("url") or tags.get("contact:url") or "")

        # Opening hours
        hours = tags.get("opening_hours") or tags.get("opening_hours:covid19") or ""

        # Category — use OSM cuisine/shop type if available
        cuisine  = tags.get("cuisine","")
        osm_cat  = tags.get("amenity") or tags.get("shop") or tags.get("leisure") or tags.get("tourism","")
        cat_name = cuisine if cuisine else (osm_cat.replace("_"," ").title() if osm_cat else category)

        # Rating / stars (some OSM entries have these)
        stars   = tags.get("stars","")
        rating  = tags.get("rating","") or (f"{stars}★" if stars else "")

        maps_url = f"https://www.openstreetmap.org/?mlat={lat}&mlon={lon}&zoom=17"

        records.append({
            "session_id":      session_id,
            "name":            name,
            "category":        cat_name,
            "address":         address,
            "city":            city,
            "state":           state,
            "country":         country,
            "latitude":        float(lat),
            "longitude":       float(lon),
            "phone":           phone,
            "website":         website,
            "rating":          rating,
            "review_count":    "",
            "opening_hours":   hours,
            "price_level":     "",
            "business_status": "OPERATIONAL",
            "maps_url":        maps_url,
            "date_scraped":    now,
        })
    return records


# ── 4. CSV chunk writer ───────────────────────────────────────────────────────
FIELDNAMES = ["name","category","address","city","state","country",
              "latitude","longitude","phone","website","rating",
              "review_count","opening_hours","price_level","business_status",
              "maps_url","date_scraped"]

def _write_chunk(records, session_id):
    if not records:
        return
    path = os.path.join(CHUNKS_DIR, f"chunk_{session_id[:8]}.csv")
    write_header = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        if write_header:
            w.writeheader()
        w.writerows(records)


# ── 5. Main scraper ───────────────────────────────────────────────────────────
def run_scraper(business_type, city, state, country, session_id=""):
    t0 = time.time()

    update_progress(
        session_id=session_id,
        business_type=business_type, city=city, state=state, country=country,
        status="geocoding", message="Finding coordinates...",
        fetched=0, inserted=0, error=""
    )

    lat, lon, display_name = geocode(city, state, country)
    if not lat:
        update_progress(status="error",
                        error=f"Could not geocode '{city}, {country}'. Check spelling.")
        return

    update_progress(status="scraping",
                    message=f"Found: {display_name[:55]}. Querying OpenStreetMap ({RADIUS_M//1000}km radius)...")

    tag_pairs = get_tag_pairs(business_type)
    query     = build_query(lat, lon, tag_pairs)

    try:
        r = requests.post(OVERPASS_URL, data={"data": query},
                          headers=HEADERS, timeout=60)
        r.raise_for_status()
        elements = r.json().get("elements", [])
    except requests.exceptions.Timeout:
        update_progress(status="error",
                        error="Overpass API timed out. Please try again in a moment.")
        return
    except Exception as e:
        update_progress(status="error", error=f"Overpass API error: {e}")
        return

    update_progress(message=f"Got {len(elements)} raw results. Parsing...")

    records       = parse_elements(elements, business_type, city, state, country, session_id)
    total_fetched = len(records)

    update_progress(fetched=total_fetched,
                    message=f"Parsed {total_fetched} businesses. Saving to database...")

    total_inserted = insert_businesses_batch(records)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        ex.submit(_write_chunk, records, session_id)

    t_total = time.time() - t0
    update_progress(
        status="done",
        fetched=total_fetched,
        inserted=total_inserted,
        message=f"Done in {t_total:.1f}s — {total_fetched} businesses found, {total_inserted} new records stored."
    )


def resume_scraper():
    prog = get_progress()
    run_scraper(prog["business_type"], prog["city"],
                prog["state"],         prog["country"],
                prog.get("session_id", ""))
