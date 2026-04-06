"""
Nightly settlement: fetch today's scores from The Odds API,
save to Supabase `ergebnisse`, then auto-settle open bets in `wetten`.

Safety rules:
  - Only settle bets whose datum <= today
  - Only use results where the API reports completed=True AND match_date <= today
  - Lookup matches by (home_team, away_team, match_date) — never by name alone
"""
import os
import requests
from datetime import date

SUPABASE_URL = "https://yloudwrsmpbtxovxozqm.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_publishable_E43hus55ruODU1i0G8FLjg_TLVAoghZ")
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")

LIGEN = {
    "Premier_League": "soccer_epl",
    "Championship":   "soccer_efl_champ",
    "2_Bundesliga":   "soccer_germany_bundesliga2",
    "Scottish_Prem":  "soccer_spl",
    "Eredivisie":     "soccer_netherlands_eredivisie",
}

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}


def norm(name):
    return name.lower().strip()


# ── 1. Scores holen und speichern ────────────────────────────────────────────

def fetch_and_save_scores():
    alle = []
    for liga, sport_key in LIGEN.items():
        url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/scores/"
        r = requests.get(url, params={"apiKey": ODDS_API_KEY, "daysFrom": 1}, timeout=15)
        print(f"  {liga}: HTTP {r.status_code}, remaining={r.headers.get('x-requests-remaining','?')}")
        if r.status_code != 200:
            print(f"    Fehler: {r.text[:200]}")
            continue
        completed = 0
        today_str = date.today().isoformat()
        for game in r.json():
            # Must be explicitly completed by the API
            if not game.get("completed"):
                continue
            # match_date must be today or in the past (guard against API quirks)
            match_date = game.get("commence_time", "")[:10]
            if match_date > today_str:
                print(f"    SKIP (Zukunft laut API): {game.get('home_team')} vs {game.get('away_team')} {match_date}")
                continue
            scores = game.get("scores") or []
            tore = {}
            for s in scores:
                try:
                    tore[s["name"]] = int(s["score"])
                except (KeyError, TypeError, ValueError):
                    pass
            home = game["home_team"]
            away = game["away_team"]
            if home not in tore or away not in tore:
                continue
            alle.append({
                "liga":        liga,
                "home_team":   home,
                "away_team":   away,
                "match_date":  match_date,
                "goals_home":  tore[home],
                "goals_away":  tore[away],
                "total_goals": tore[home] + tore[away],
            })
            completed += 1
        print(f"    -> {completed} abgeschlossene Spiele (Datum <= heute)")

    if not alle:
        print("  Keine abgeschlossenen Spiele gefunden.")
        return []

    # Idempotent: heute löschen, dann neu einfügen
    today = date.today().isoformat()
    requests.delete(
        f"{SUPABASE_URL}/rest/v1/ergebnisse",
        headers={**SB_HEADERS, "Prefer": ""},
        params={"match_date": f"eq.{today}"},
    )
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/ergebnisse",
        headers=SB_HEADERS,
        json=alle,
    )
    print(f"  -> Supabase ergebnisse: HTTP {r.status_code} ({len(alle)} Einträge gespeichert)")
    return alle


# ── 2. Offene Wetten abrechnen ────────────────────────────────────────────────

def get_open_wetten():
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/wetten",
        headers=SB_HEADERS,
        params={"status": "eq.offen", "select": "*", "order": "datum.asc"},
    )
    if r.status_code != 200:
        print(f"  Fehler beim Laden der Wetten: {r.status_code} {r.text}")
        return []
    return r.json()


def get_current_bankroll():
    # Zuletzt abgerechnete Wette -> bankroll_danach
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/wetten",
        headers=SB_HEADERS,
        params={"status": "neq.offen", "select": "bankroll_danach",
                "order": "created_at.desc", "limit": 1},
    )
    if r.status_code == 200:
        rows = r.json()
        if rows and rows[0].get("bankroll_danach"):
            return float(rows[0]["bankroll_danach"])
    # Fallback: bankroll Tabelle
    r2 = requests.get(
        f"{SUPABASE_URL}/rest/v1/bankroll",
        headers=SB_HEADERS,
        params={"select": "betrag", "order": "created_at.desc", "limit": 1},
    )
    if r2.status_code == 200 and r2.json():
        return float(r2.json()[0]["betrag"])
    return 50.0


def settle_wette(wette_id, status, tore_heim, tore_ausw, gewinn, bankroll_danach):
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/wetten",
        headers=SB_HEADERS,
        params={"id": f"eq.{wette_id}"},
        json={
            "status":         status,
            "tore_heim":      tore_heim,
            "tore_ausw":      tore_ausw,
            "gewinn":         round(gewinn, 2),
            "bankroll_danach": round(bankroll_danach, 2),
        },
    )
    return r.status_code


def settle_open_wetten(ergebnisse):
    today = date.today()
    today_str = today.isoformat()

    # Lookup: (norm_home, norm_away, match_date) -> ergebnis
    # Date is part of the key — prevents cross-date false matches
    lookup = {}
    for e in ergebnisse:
        key = (norm(e["home_team"]), norm(e["away_team"]), e["match_date"])
        lookup[key] = e

    wetten = get_open_wetten()
    print(f"  {len(wetten)} offene Wetten")
    if not wetten:
        print("  Nichts zu tun.")
        return

    bankroll = get_current_bankroll()
    print(f"  Aktueller Bankroll: {bankroll:.2f}EUR\n")

    abgerechnet = 0
    for wette in wetten:
        heim     = wette["heim"]
        ausw     = wette["ausw"]
        datum    = wette.get("datum", "")
        richtung = wette["richtung"]
        einsatz  = float(wette["einsatz"])
        quote    = float(wette["quote"])

        # Regel 1: Spieldatum muss heute oder in der Vergangenheit liegen
        if not datum or datum > today_str:
            print(f"  SKIP (Zukunft): {heim} vs {ausw} ({datum})")
            continue

        # Regel 2: Lookup by (teams + exact date) — no cross-date matching
        ergebnis = lookup.get((norm(heim), norm(ausw), datum))
        swapped  = False
        if not ergebnis:
            ergebnis = lookup.get((norm(ausw), norm(heim), datum))
            swapped  = True

        if not ergebnis:
            print(f"  ? Kein Ergebnis für: {heim} vs {ausw} ({datum})")
            continue

        gesamt    = ergebnis["total_goals"]
        tore_heim = ergebnis["goals_away"] if swapped else ergebnis["goals_home"]
        tore_ausw = ergebnis["goals_home"] if swapped else ergebnis["goals_away"]

        ueber_gewonnen = gesamt > 2.5
        gewonnen = ueber_gewonnen if richtung == "ueber" else not ueber_gewonnen

        status = "gewonnen" if gewonnen else "verloren"
        if gewonnen:
            gewinn   = round(einsatz * (quote - 1), 2)
            bankroll = round(bankroll + gewinn, 2)
        else:
            gewinn   = -einsatz
            bankroll = round(bankroll - einsatz, 2)

        sc = settle_wette(wette["id"], status, tore_heim, tore_ausw, gewinn, bankroll)
        sym = "OK" if gewonnen else "XX"
        print(f"  {sym} {heim} vs {ausw}: {tore_heim}:{tore_ausw} "
              f"({gesamt} Tore) -> {status} | {gewinn:+.2f}€ | "
              f"Bankroll {bankroll:.2f}€ [HTTP {sc}]")
        abgerechnet += 1

    print(f"\n  {abgerechnet} Wetten abgerechnet. Neuer Bankroll: {bankroll:.2f}€")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Ergebnisauswertung ===\n")

    print("1) Spielergebnisse laden und speichern...")
    ergebnisse = fetch_and_save_scores()

    print("\n2) Offene Wetten abrechnen...")
    settle_open_wetten(ergebnisse)

    print("\nFertig.")
