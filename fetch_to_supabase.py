import requests
import csv
import io
import os
from datetime import datetime

SUPABASE_URL = "https://yloudwrsmpbtxovxozqm.supabase.co"
# Setze SUPABASE_KEY als Umgebungsvariable (service_role key aus Supabase Dashboard)
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

FIXTURES_URL = "https://www.football-data.co.uk/fixtures.csv"

DIV_TO_LIGA = {
    "E0":  "Premier_League",
    "E1":  "Championship",
    "D2":  "2_Bundesliga",
    "SC0": "Scottish_Prem",
    "N1":  "Eredivisie",
}

def fetch_fixtures():
    print("Lade fixtures.csv...")
    r = requests.get(FIXTURES_URL, timeout=15)
    r.raise_for_status()
    # utf-8-sig entfernt BOM (\ufeff) automatisch
    text = r.content.decode('utf-8-sig')
    reader = csv.DictReader(io.StringIO(text))
    nach_liga = {liga: [] for liga in DIV_TO_LIGA.values()}
    for row in reader:
        div = row.get("Div", "").strip()
        liga = DIV_TO_LIGA.get(div)
        if not liga:
            continue
        if not row.get("HomeTeam") or not row.get("AwayTeam"):
            continue
        nach_liga[liga].append({
            "HomeTeam": row.get("HomeTeam", ""),
            "AwayTeam": row.get("AwayTeam", ""),
            "Date":     row.get("Date", ""),
            "Avg>2.5":  row.get("Avg>2.5", ""),
            "Avg<2.5":  row.get("Avg<2.5", ""),
        })
    for liga, spiele in nach_liga.items():
        print(f"  {liga}: {len(spiele)} Spiele")
    return nach_liga

def save_to_supabase(liga_key, spiele):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    data = {
        "liga": liga_key,
        "spiele": spiele,
        "updated_at": datetime.now().isoformat(),
    }
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/signale_cache?on_conflict=liga",
        headers=headers,
        json=data,
    )
    print(f"  {liga_key}: HTTP {r.status_code} – {len(spiele)} Spiele")
    if r.status_code not in [200, 201]:
        print(f"  Fehler: {r.text}")

if not SUPABASE_KEY:
    print("FEHLER: SUPABASE_KEY nicht gesetzt. Bitte service_role key aus Supabase Dashboard als Umgebungsvariable setzen.")
    exit(1)

nach_liga = fetch_fixtures()
for liga, spiele in nach_liga.items():
    key = f"fix_{liga}"
    print(f"Speichere {key}...")
    save_to_supabase(key, spiele)

print("Fertig.")
