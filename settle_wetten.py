"""
Nightly settlement: fetch completed scores from The Odds API,
save to Supabase `ergebnisse`, then auto-settle open bets in `wetten`.

A bet is settled ONLY when ALL three conditions are simultaneously true:
  1. completed == True  (boolean True, not just truthy)
  2. scores is a non-empty list with entries for both home and away team
  3. Both home_score and away_score are valid integers >= 0 (real data, not placeholders)

Additionally, two independent date-safety layers:
  Layer 1 - Score fetch : match_date must be strictly in the past (< today UTC)
                          Today's and future games are NEVER stored — even if completed=True
  Layer 2 - Bet settle  : datum must be strictly in the past (< today UTC)
                          Today's and future bets are NEVER touched
  Lookup key            : (home_team, away_team, match_date) — no cross-date matching

Root cause of past bugs:
  - Odds API returns completed=True with 0:0 placeholder scores for today's unplayed games
  - Manual workflow triggers during the day had today_str == match_date, but old code
    used strict > instead of >= so today's games slipped through
"""
import os
import requests
from datetime import date, timedelta, timezone, datetime

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


def today_utc():
    """Always use UTC date — consistent with GitHub Actions runner and API timestamps."""
    return datetime.now(timezone.utc).date().isoformat()


# ── 1. Scores holen und speichern ────────────────────────────────────────────

def fetch_and_save_scores():
    today_str     = today_utc()
    yesterday_str = (date.fromisoformat(today_str) - timedelta(days=1)).isoformat()
    print(f"  Heute (UTC): {today_str}")
    print(f"  Regel: match_date < {today_str} UND completed=True UND Scores vorhanden")
    print()

    alle = []
    for liga, sport_key in LIGEN.items():
        url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/scores/"
        r = requests.get(url, params={"apiKey": ODDS_API_KEY, "daysFrom": 2}, timeout=15)
        print(f"  [{liga}] HTTP {r.status_code}, remaining={r.headers.get('x-requests-remaining','?')}")
        if r.status_code != 200:
            print(f"    Fehler: {r.text[:200]}")
            continue

        akzeptiert = 0
        for game in r.json():
            home       = game.get("home_team", "")
            away       = game.get("away_team", "")
            completed  = game.get("completed", False)
            match_date = game.get("commence_time", "")[:10]
            scores_raw = game.get("scores")

            label = f"    {home} vs {away} | {match_date} | completed={completed}"

            # CHECK A: completed muss boolean True sein
            if completed is not True:
                print(f"{label}")
                print(f"      -> SKIP: completed={completed!r} (kein boolean True)")
                continue

            # CHECK B: match_date muss strikt in der Vergangenheit liegen
            # >= bedeutet: heute UND Zukunft sind VERBOTEN
            if match_date >= today_str:
                print(f"{label}")
                print(f"      -> SKIP: match_date={match_date} >= heute={today_str} (heute/Zukunft verboten)")
                continue

            # CHECK C: scores muss eine nicht-leere Liste sein
            if not scores_raw or not isinstance(scores_raw, list) or len(scores_raw) < 2:
                print(f"{label}")
                print(f"      -> SKIP: scores fehlen oder leer: {scores_raw!r}")
                continue

            # CHECK D: Beide Scores muessen gueltige nicht-negative Integers sein
            tore = {}
            valid = True
            for s in scores_raw:
                team_name = s.get("name", "")
                try:
                    score_val = int(s["score"])
                    if score_val < 0:
                        raise ValueError("negatives Ergebnis")
                    tore[team_name] = score_val
                except (KeyError, TypeError, ValueError) as e:
                    print(f"{label}")
                    print(f"      -> SKIP: Score nicht parsebar fuer {team_name!r}: {s!r} ({e})")
                    valid = False
                    break

            if not valid:
                continue

            if home not in tore or away not in tore:
                print(f"{label}")
                print(f"      -> SKIP: Kein Score fuer {home!r} oder {away!r} in {scores_raw!r}")
                continue

            goals_home = tore[home]
            goals_away = tore[away]
            total      = goals_home + goals_away

            # CHECK E: Redundanter Fallback — 0:0 fuer heutige/zukuenftige Spiele = Platzhalter
            # Die Odds API setzt manchmal completed=True mit 0:0 fuer ungespielte Spiele.
            # Obwohl CHECK B das abfangen sollte, schuetzt dieser Check falls CHECK B versagt.
            if goals_home == 0 and goals_away == 0 and match_date >= today_str:
                print(f"{label} | 0:0")
                print(f"      -> SKIP: 0:0 Platzhalter-Score bei heutigem/zukuenftigem Spiel "
                      f"(match_date={match_date} >= heute={today_str})")
                continue

            print(f"{label} | {goals_home}:{goals_away}")
            print(f"      -> AKZEPTIERT: {total} Tore gesamt")

            alle.append({
                "liga":        liga,
                "home_team":   home,
                "away_team":   away,
                "match_date":  match_date,
                "goals_home":  goals_home,
                "goals_away":  goals_away,
                "total_goals": total,
            })
            akzeptiert += 1

        print(f"  [{liga}] Gesamt akzeptiert: {akzeptiert}\n")

    if not alle:
        print("  Keine verwertbaren Ergebnisse gefunden.")
        return []

    # Idempotent: gestrige Eintraege loeschen und neu schreiben
    del_r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/ergebnisse",
        headers={**SB_HEADERS, "Prefer": ""},
        params={"match_date": f"eq.{yesterday_str}"},
    )
    print(f"  Supabase delete yesterday ({yesterday_str}): HTTP {del_r.status_code}")

    ins_r = requests.post(
        f"{SUPABASE_URL}/rest/v1/ergebnisse",
        headers=SB_HEADERS,
        json=alle,
    )
    print(f"  Supabase insert: HTTP {ins_r.status_code} ({len(alle)} Eintraege)")
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
    today_str = today_utc()

    # Lookup: (norm_home, norm_away, match_date)
    # Doppelte Absicherung: Ergebnisse mit match_date >= heute werden NICHT in Lookup aufgenommen
    lookup = {}
    skipped_lookup = 0
    for e in ergebnisse:
        if e["match_date"] >= today_str:
            print(f"  [LOOKUP-SKIP] {e['home_team']} vs {e['away_team']} | "
                  f"match_date={e['match_date']} >= heute={today_str}")
            skipped_lookup += 1
            continue
        key = (norm(e["home_team"]), norm(e["away_team"]), e["match_date"])
        lookup[key] = e

    print(f"  {len(lookup)} Ergebnisse im Lookup (match_date < {today_str}), "
          f"{skipped_lookup} durch doppelten Datumsschutz uebersprungen")

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

        # LAYER 2: datum muss strikt in der Vergangenheit liegen
        # >= bedeutet: heute UND Zukunft werden NIEMALS angefasst
        if not datum or datum >= today_str:
            print(f"  [SKIP] {heim} vs {ausw} | datum={datum!r} >= heute={today_str} "
                  f"(heute/Zukunft — kein Ergebnis eintragen)")
            continue

        # Lookup by (teams + exact match_date) — kein Kreuz-Datum-Matching moeglich
        ergebnis = lookup.get((norm(heim), norm(ausw), datum))
        swapped  = False
        if not ergebnis:
            ergebnis = lookup.get((norm(ausw), norm(heim), datum))
            swapped  = True

        if not ergebnis:
            print(f"  [WARTE] Kein Ergebnis fuer: {heim} vs {ausw} ({datum}) — "
                  f"wird beim naechsten Lauf erneut versucht")
            continue

        goals_home_raw = ergebnis["goals_away"] if swapped else ergebnis["goals_home"]
        goals_away_raw = ergebnis["goals_home"] if swapped else ergebnis["goals_away"]
        gesamt         = ergebnis["total_goals"]

        ueber_gewonnen = gesamt > 2.5
        gewonnen = ueber_gewonnen if richtung == "ueber" else not ueber_gewonnen

        status = "gewonnen" if gewonnen else "verloren"
        if gewonnen:
            gewinn   = round(einsatz * (quote - 1), 2)
            bankroll = round(bankroll + gewinn, 2)
        else:
            gewinn   = -einsatz
            bankroll = round(bankroll - einsatz, 2)

        sc  = settle_wette(wette["id"], status, goals_home_raw, goals_away_raw, gewinn, bankroll)
        sym = "OK" if gewonnen else "XX"
        print(f"  [{sym}] {heim} vs {ausw} ({datum}): "
              f"{goals_home_raw}:{goals_away_raw} ({gesamt} Tore) "
              f"| {richtung} 2.5 | {status} | {gewinn:+.2f} EUR | "
              f"Bankroll {bankroll:.2f} EUR [HTTP {sc}]")
        abgerechnet += 1

    print(f"\n  {abgerechnet} Wetten abgerechnet. Neuer Bankroll: {bankroll:.2f} EUR")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Ergebnisauswertung ===")
    print(f"Laufzeit: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()

    print("1) Spielergebnisse laden und speichern...")
    print("-" * 60)
    ergebnisse = fetch_and_save_scores()

    print()
    print("2) Offene Wetten abrechnen...")
    print("-" * 60)
    settle_open_wetten(ergebnisse)

    print()
    print("Fertig.")
