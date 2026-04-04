import requests
import os
from datetime import datetime, timedelta

SUPABASE_URL = "https://yloudwrsmpbtxovxozqm.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_publishable_E43hus55ruODU1i0G8FLjg_TLVAoghZ")
API_KEY = os.environ.get("API_FOOTBALL_KEY", "")

API_BASE = "https://v3.football.api-sports.io"
API_HEADERS = {
    "x-apisports-key": API_KEY,
}

LIGEN = {
    "Premier_League": 39,
    "Championship":   40,
    "2_Bundesliga":   81,
    "Scottish_Prem":  179,
    "Eredivisie":     88,
}

SAISON = 2024  # aktuelle Saison


def fetch_fixtures(league_id):
    heute = datetime.now().strftime("%Y-%m-%d")
    bis = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
    r = requests.get(
        f"{API_BASE}/fixtures",
        headers=API_HEADERS,
        params={"league": league_id, "season": SAISON, "from": heute, "to": bis, "status": "NS"},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("errors"):
        print(f"  API Fehler: {data['errors']}")
        return []
    return data.get("response", [])


def fetch_odds(fixture_id):
    r = requests.get(
        f"{API_BASE}/odds",
        headers=API_HEADERS,
        params={"fixture": fixture_id, "bet": 5},  # bet 5 = Goals Over/Under
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    bookmakers = data.get("response", [{}])[0].get("bookmakers", []) if data.get("response") else []
    for bm in bookmakers:
        for bet in bm.get("bets", []):
            if "over" in bet.get("name", "").lower() or bet.get("id") == 5:
                values = {v["value"]: float(v["odd"]) for v in bet.get("values", [])}
                over = values.get("Over 2.5") or values.get("Over")
                under = values.get("Under 2.5") or values.get("Under")
                if over and under:
                    return round(over, 2), round(under, 2)
    return None, None


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
    print(f"  Supabase {liga_key}: HTTP {r.status_code} — {len(spiele)} Spiele gespeichert")
    if r.status_code not in [200, 201]:
        print(f"  Fehler: {r.text}")


if not API_KEY:
    print("FEHLER: API_FOOTBALL_KEY nicht gesetzt.")
    exit(1)

# Verbleibende API-Anfragen prüfen
status = requests.get(f"{API_BASE}/status", headers=API_HEADERS, timeout=10).json()
print(f"API Status: {status.get('response', {}).get('requests', {})}")

for liga_name, league_id in LIGEN.items():
    print(f"\n[{liga_name}] Lade Fixtures (league={league_id})...")
    fixtures = fetch_fixtures(league_id)
    print(f"  {len(fixtures)} Spiele gefunden")

    spiele = []
    for fix in fixtures:
        f = fix.get("fixture", {})
        teams = fix.get("teams", {})
        fixture_id = f.get("id")
        date_str = f.get("date", "")  # ISO: "2026-04-06T14:00:00+00:00"

        # Datum in DD/MM/YYYY + Zeit umwandeln
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            date_fmt = dt.strftime("%d/%m/%Y")
            time_fmt = dt.strftime("%H:%M")
        except Exception:
            date_fmt = date_str[:10]
            time_fmt = ""

        # Quoten laden
        qO, qU = fetch_odds(fixture_id)
        print(f"  {teams.get('home',{}).get('name')} vs {teams.get('away',{}).get('name')} {date_fmt} {time_fmt} — Über:{qO} Unter:{qU}")

        spiele.append({
            "HomeTeam": teams.get("home", {}).get("name", ""),
            "AwayTeam": teams.get("away", {}).get("name", ""),
            "Date":     date_fmt,
            "Time":     time_fmt,
            "Avg>2.5":  str(qO) if qO else "",
            "Avg<2.5":  str(qU) if qU else "",
        })

    key = f"fix_{liga_name}"
    save_to_supabase(key, spiele)

print("\nFertig.")
