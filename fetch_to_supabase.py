import os
import requests
import json
from datetime import datetime, timezone

SUPABASE_URL = "https://yloudwrsmpbtxovxozqm.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_publishable_E43hus55ruODU1i0G8FLjg_TLVAoghZ")
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")

LIGEN = {
    "Premier_League":  "soccer_epl",
    "Championship":    "soccer_efl_champ",
    "2_Bundesliga":    "soccer_germany_bundesliga2",
    "Scottish_Prem":   "soccer_spl",
    "Eredivisie":      "soccer_netherlands_eredivisie",
}

def fetch_odds(liga_key, sport_key):
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds/"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "eu",
        "markets": "totals",
        "oddsFormat": "decimal",
    }
    r = requests.get(url, params=params)
    print(f"  API Status: {r.status_code}, Requests remaining: {r.headers.get('x-requests-remaining','?')}")
    if r.status_code != 200:
        print(f"  Fehler: {r.text}")
        return []
    
    spiele = []
    for game in r.json():
        home = game.get("home_team", "")
        away = game.get("away_team", "")
        commence = game.get("commence_time", "")
        
        avg_over = ""
        avg_under = ""
        
        # Tipico bevorzugen, sonst Durchschnitt berechnen
        tipico = next((b for b in game.get("bookmakers", []) if b["key"] == "tipico_de"), None)
        if tipico:
            for market in tipico.get("markets", []):
                if market["key"] == "totals":
                    for o in market["outcomes"]:
                        if o["name"] == "Over" and o.get("point") == 2.5:
                            avg_over = str(o["price"])
                        if o["name"] == "Under" and o.get("point") == 2.5:
                            avg_under = str(o["price"])
        
        # Fallback: Durchschnitt aller Buchmacher
        if not avg_over or not avg_under:
            over_prices = []
            under_prices = []
            for bm in game.get("bookmakers", []):
                for market in bm.get("markets", []):
                    if market["key"] == "totals":
                        for o in market["outcomes"]:
                            if o.get("point") == 2.5:
                                if o["name"] == "Over":
                                    over_prices.append(o["price"])
                                if o["name"] == "Under":
                                    under_prices.append(o["price"])
            if over_prices:
                avg_over = str(round(sum(over_prices)/len(over_prices), 2))
            if under_prices:
                avg_under = str(round(sum(under_prices)/len(under_prices), 2))
        
        spiele.append({
            "HomeTeam": home,
            "AwayTeam": away,
            "Date": commence[:10],
            "Time": commence[11:16],
            "Avg>2.5": avg_over,
            "Avg<2.5": avg_under,
        })
    
    print(f"  {liga_key}: {len(spiele)} Spiele mit Quoten")
    return spiele

def save_to_supabase(table, data):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    # Erst löschen
    requests.delete(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**headers, "Prefer": ""},
        params={"id": "neq.0"}
    )
    # Dann neu speichern
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=headers,
        json=data
    )
    print(f"  Supabase {table}: HTTP {r.status_code}")

for liga, sport_key in LIGEN.items():
    print(f"\nLade {liga}...")
    spiele = fetch_odds(liga, sport_key)
    if spiele:
        save_to_supabase(f"fix_{liga}", spiele)

print("\nFertig.")