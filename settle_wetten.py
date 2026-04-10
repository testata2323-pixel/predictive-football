"""
Nightly settlement: fetch today's scores from The Odds API,
save to Supabase `ergebnisse`, then auto-settle open bets in `wetten`.

Safety rules (two independent layers):
  Layer 1 - Score fetch:   only store results where completed=True AND match_date <= today
  Layer 2 - Bet settle:    skip any wette whose datum > today, regardless of results
  Lookup key:              (home_team, away_team, match_date) — never by name alone
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
    today_str = date.today().isoformat()
    print(f"  Datum heute: {today_str}")

    alle = []
    for liga, sport_key in LIGEN.items():
        url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/scores/"
        r = requests.get(url, params={"apiKey": ODDS_API_KEY, "daysFrom": 1}, timeout=15)
        print(f"  {liga}: HTTP {r.status_code}, remaining={r.headers.get('x-requests-remaining','?')}")
        if r.status_code != 200:
            print(f"    Fehler: {r.text[:200]}")
            continue

        akzeptiert = 0
        for game in r.json():
            home       = game.get("home_team", "")
            away       = game.get("away_team", "")
            completed  = game.get("completed", False)
            match_date = game.get("commence_time", "")[:10]

            # LAYER 1A: API muss completed=True melden
            if not completed:
                continue

            # LAYER 1B: Spieldatum darf nicht in der Zukunft liegen
            if match_date > today_str:
                print(f"    SKIP (completed=True aber Datum {match_date} > heute): {home} vs {away}")
                continue

            scores = game.get("scores") or []
            tore = {}
            for s in scores:
                try:
                    tore[s["name"]] = int(s["score"])
                except (KeyError, TypeError, ValueError):
                    pass

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
            akzeptiert += 1

        print(f"    -> {akzeptiert} abgeschlossene Spiele (completed=True, Datum <= heute)")

    if not alle:
        print("  Keine verwertbaren Ergebnisse gefunden.")
        return []

    # Idempotent: heute loeschen, dann neu einfuegen
    requests.delete(
        f"{SUPABASE_URL}/rest/v1/ergebnisse",
        headers={**SB_HEADERS, "Prefer": ""},
        params={"match_date": f"eq.{today_str}"},
    )
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/ergebnisse",
        headers=SB_HEADERS,
        json=alle,
    )
    print(f"  -> Supabase ergebnisse: HTTP {r.status_code} ({len(alle)} Eintraege)")
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
            "status":          status,
            "tore_heim":       tore_heim,
            "tore_ausw":       tore_ausw,
            "gewinn":          round(gewinn, 2),
            "bankroll_danach": round(bankroll_danach, 2),
        },
    )
    return r.status_code


def settle_open_wetten(ergebnisse):
    today_str = date.today().isoformat()

    # Lookup: (norm_home, norm_away, match_date) — Datum ist Teil des Schluessels
    # Extra-Filter: nur Ergebnisse mit match_date <= heute in den Lookup aufnehmen
    lookup = {}
    for e in ergebnisse:
        if e["match_date"] > today_str:
            continue  # doppelte Absicherung
        key = (norm(e["home_team"]), norm(e["away_team"]), e["match_date"])
        lookup[key] = e

    print(f"  {len(lookup)} Ergebnisse im Lookup (Datum <= {today_str})")

    wetten = get_open_wetten()
    print(f"  {len(wetten)} offene Wetten\n")
    if not wetten:
        print("  Nichts zu tun.")
        return

    bankroll = get_current_bankroll()
    print(f"  Aktueller Bankroll: {bankroll:.2f} EUR\n")

    abgerechnet = 0
    for wette in wetten:
        heim     = wette["heim"]
        ausw     = wette["ausw"]
        datum    = wette.get("datum") or ""
        richtung = wette["richtung"]
        einsatz  = float(wette["einsatz"])
        quote    = float(wette["quote"])

        # LAYER 2: Spieldatum der Wette muss <= heute sein
        if not datum or datum > today_str:
            print(f"  SKIP (Spieldatum in Zukunft): {heim} vs {ausw} | {datum} > {today_str}")
            continue

        # Lookup by (teams + exact date) — verhindert Kreuz-Datum-Matches
        ergebnis = lookup.get((norm(heim), norm(ausw), datum))
        swapped  = False
        if not ergebnis:
            ergebnis = lookup.get((norm(ausw), norm(heim), datum))
            swapped  = True

        if not ergebnis:
            print(f"  WARTE: Noch kein Ergebnis fuer: {heim} vs {ausw} ({datum})")
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
              f"({gesamt} Tore) -> {status} | {gewinn:+.2f} EUR | "
              f"Bankroll {bankroll:.2f} EUR [HTTP {sc}]")
        abgerechnet += 1

    print(f"\n  {abgerechnet} Wetten abgerechnet. Neuer Bankroll: {bankroll:.2f} EUR")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Ergebnisauswertung ===\n")

    print("1) Spielergebnisse laden und speichern...")
    ergebnisse = fetch_and_save_scores()

    print("\n2) Offene Wetten abrechnen...")
    settle_open_wetten(ergebnisse)

    print("\nFertig.")
