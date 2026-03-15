# BizExtract — Business Data Harvester

A web-based automated business data extraction system using OpenStreetMap, Overpass API, and Nominatim.

## Features
- Extract business data for any city worldwide
- Stores unique records in a local SQLite database
- Resume interrupted scraping sessions
- Export results to Excel (.xlsx)
- Clean, dark-themed web dashboard

## Setup

### 1. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the app
```bash
python app.py
```

### 3. Open in browser
Visit: http://localhost:5000

## Usage

1. Enter **Business Type** (e.g. cafe, restaurant, pharmacy, hotel)
2. Enter **City**, **State** (optional), **Country**
3. Click **Start Scraping**
4. Watch live progress in the status panel
5. Click **Export Excel** when done

## Project Structure

```
bizextract/
├── app.py           # Flask web app
├── scraper.py       # Overpass API scraper + Nominatim geocoder
├── database.py      # SQLite database layer
├── exporter.py      # Excel export
├── progress.py      # Progress tracking (JSON)
├── requirements.txt
├── templates/
│   └── index.html   # Web dashboard UI
├── chunks/          # CSV chunk files (auto-created)
├── businesses.db    # SQLite DB (auto-created)
├── progress.json    # Scraping state (auto-created)
└── business_data_export.xlsx  # Export output
```

## Data Fields
- Business Name
- Category
- Address
- City / State / Country
- Latitude / Longitude
- OpenStreetMap URL
- Date Scraped

## Notes
- Uses free/open APIs — no billing or API keys required
- OSM coverage varies by region; urban areas have best results
- Large cities may return 100–500+ businesses per query
