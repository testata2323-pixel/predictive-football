import requests
import csv
import io
import os
from datetime import datetime

SUPABASE_URL = "https://yloudwrsmpbtxovxozqm.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_publishable_E43hus55ruODU1i0G8FLjg_TLVAoghZ")

FIXTURES_URL = "https://www.football-data.co.uk/fixtures.csv"

LIGEN = {
    "Premier_League": "https://www.football-data.co.uk/mmz4281/2526/E0.csv",
    "Championship":   "https://www.football-data.co.uk/mmz4281/2526/E1.csv",
    "2_Bundesliga":   "https://www.football-data.co.uk/mmz4281/2526/D2.csv",
    "Scottish_Prem":  "https://www.football-data.co.uk/mmz4281/2526/SC0.csv",
    "Eredivisie":     "https://www.football-data.co.uk/mmz4281/2526/N1.csv",
}

DIV_TO_LIGA = {
    "E0":  "Premier_League",
    "E1":  "Championship",
    "D2":  "2_Bundesliga",
    "SC0": "Scottish_Prem",
    "N1":  "Eredivisie",
}


def fetch_csv(url):
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return csv.DictReader(io.StringIO(r.content.decode("utf-8-sig")))


def fetch_fixtures():
    print("Lade fixtures.csv...")
    reader = fetch_csv(FIXTURES_URL)
    nach_liga = {liga: [] for liga in DIV_TO_LIGA.values()}
    for row in reader:
        liga = DIV_TO_LIGA.get(row.get("Div", "").strip())
        if not liga or not row.get("HomeTeam") or not row.get("AwayTeam"):
            continue
        nach_liga[liga].append({
            "HomeTeam": row["HomeTeam"].strip(),
            "AwayTeam": row["AwayTeam"].strip(),
            "Date":     row.get("Date", "").strip(),
            "Time":     row.get("Time", "").strip(),
        })
    for liga, spiele in nach_liga.items():
        print(f"  {liga}: {len(spiele)} Fixtures")
    return nach_liga


def fetch_odds(liga, url):
    print(f"Lade Saison-CSV für {liga}...")
    reader = fetch_csv(url)
    odds = {}
    for row in reader:
        heim = row.get("HomeTeam", "").strip()
        ausw = row.get("AwayTeam", "").strip()
        if not heim or not ausw:
            continue
        avg_over  = row.get("Avg>2.5", "").strip()
        avg_under = row.get("Avg<2.5", "").strip()
        b365_over  = row.get("B365>2.5", "").strip()
        b365_under = row.get("B365<2.5", "").strip()
        key = (heim.lower(), ausw.lower())
        odds[key] = {
            "Avg>2.5": avg_over  or b365_over,
            "Avg<2.5": avg_under or b365_under,
        }
    print(f"  {len(odds)} Spiele mit Quoten im Saison-CSV")
    return odds


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
    print(f"  Supabase {liga_key}: HTTP {r.status_code} — {len(spiele)} Spiele")
    if r.status_code not in [200, 201]:
        print(f"  Fehler: {r.text}")


fixtures_nach_liga = fetch_fixtures()

for liga, saison_url in LIGEN.items():
    fixtures = fixtures_nach_liga.get(liga, [])
    odds = fetch_odds(liga, saison_url)

    spiele = []
    matched = 0
    for f in fixtures:
        key = (f["HomeTeam"].lower(), f["AwayTeam"].lower())
        o = odds.get(key, {})
        if o.get("Avg>2.5") or o.get("Avg<2.5"):
            matched += 1
        spiele.append({
            "HomeTeam": f["HomeTeam"],
            "AwayTeam": f["AwayTeam"],
            "Date":     f["Date"],
            "Time":     f["Time"],
            "Avg>2.5":  o.get("Avg>2.5", ""),
            "Avg<2.5":  o.get("Avg<2.5", ""),
        })

    print(f"  {liga}: {len(spiele)} Fixtures, {matched} mit Quoten gematcht")
    save_to_supabase(f"fix_{liga}", spiele)

print("\nFertig.")
