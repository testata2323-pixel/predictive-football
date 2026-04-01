"""
PREDICTIVE FOOTBALL - TÄGLICHES UPDATE SCRIPT
===============================================
Läuft täglich automatisch - passt sich dem Wochentag an:

Mo: Vollanalyse + Strategieanpassung (wöchentlich)
Di-Do: Midweek-Check + Verletzungsupdates
Fr 14h: Wochenend-Empfehlungen + vollständiger Verletzungscheck
Sa/So: Stündlich bis Spielbeginn - Quoten + Aufstellungen

AUSFUEHREN: python 17_daily_update.py
AUTOMATISCH: Windows Task Scheduler oder Cron Job

Windows Task Scheduler einrichten:
1. Windows-Taste → "Aufgabenplanung" öffnen
2. "Einfache Aufgabe erstellen"
3. Täglich, 08:00 Uhr
4. Programm: python.exe
5. Argumente: D:\\Projects\\Privat\\predictive-football\\17_daily_update.py
"""

import requests
import pandas as pd
import numpy as np
import json
import io
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# KONFIGURATION
# ============================================================

import os

# Keys aus Umgebungsvariablen (GitHub Secrets) oder lokal aus config
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://yloudwrsmpbtxovxozqm.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', 'sb_publishable_E43hus55ruODU1i0G8FLjg_TLVAoghZ')
API_FOOTBALL_KEY = os.environ.get('API_FOOTBALL_KEY', 'DEIN_API_FOOTBALL_KEY')

HEADERS_SB = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
}

HEADERS_API = {
    'x-apisports-key': API_FOOTBALL_KEY,
    'x-apisports-host': 'v3.football.api-sports.io'
}

CONFIG_DATEI = 'strategie_config.json'
STARTKAPITAL = 50.0

LIGA_URLS = {
    'Premier_League': 'https://www.football-data.co.uk/mmz4281/2425/E0.csv',
    '2_Bundesliga':   'https://www.football-data.co.uk/mmz4281/2425/D2.csv',
    'Scottish_Prem':  'https://www.football-data.co.uk/mmz4281/2425/SC0.csv',
    'Championship':   'https://www.football-data.co.uk/mmz4281/2425/E1.csv',
    'Eredivisie':     'https://www.football-data.co.uk/mmz4281/2425/N1.csv',
}

LIGA_IDS = {
    'Premier_League': 39,
    '2_Bundesliga': 79,
    'Scottish_Prem': 179,
    'Championship': 40,
    'Eredivisie': 88,
}

WOCHENTAGE_DE = ['Mo','Di','Mi','Do','Fr','Sa','So']

# ============================================================
# HILFSFUNKTIONEN
# ============================================================

def sb_get(tabelle, query=''):
    url = f"{SUPABASE_URL}/rest/v1/{tabelle}?{query}"
    r = requests.get(url, headers=HEADERS_SB, timeout=10)
    return r.json() if r.status_code == 200 else []

def sb_update(tabelle, id_val, daten):
    url = f"{SUPABASE_URL}/rest/v1/{tabelle}?id=eq.{id_val}"
    r = requests.patch(url, headers=HEADERS_SB, json=daten, timeout=10)
    return r.status_code in [200, 204]

def sb_insert(tabelle, daten):
    url = f"{SUPABASE_URL}/rest/v1/{tabelle}"
    r = requests.post(url, headers=HEADERS_SB, json=daten, timeout=10)
    return r.status_code in [200, 201]

def lade_config():
    if os.path.exists(CONFIG_DATEI):
        with open(CONFIG_DATEI, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def speichere_config(config):
    config['letzte_aktualisierung'] = datetime.now().strftime('%d.%m.%Y %H:%M')
    with open(CONFIG_DATEI, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

def lade_csv(url):
    try:
        r = requests.get(url, timeout=15)
        df = pd.read_csv(io.StringIO(r.text), low_memory=False)
        return df.dropna(subset=['HomeTeam', 'AwayTeam'])
    except:
        return None

def api_football(endpoint, params={}):
    """API-Football Anfrage (max 100/Tag im Free Plan)."""
    try:
        r = requests.get(
            f"https://v3.football.api-sports.io/{endpoint}",
            headers=HEADERS_API, params=params, timeout=10
        )
        data = r.json()
        if data.get('errors'):
            return None
        return data.get('response', [])
    except:
        return None

# ============================================================
# MODUS BESTIMMEN
# ============================================================

now = datetime.now()
wochentag = now.weekday()  # 0=Mo, 6=So
stunde = now.hour
wochentag_name = WOCHENTAGE_DE[wochentag]

print("="*65)
print(f"  PREDICTIVE FOOTBALL - TÄGLICHES UPDATE")
print(f"  {now.strftime('%d.%m.%Y %H:%M')} | {wochentag_name}")
print("="*65)
print()

# Modus festlegen
if wochentag == 0:
    MODUS = 'vollanalyse'
elif wochentag in [1, 2, 3]:
    MODUS = 'midweek'
elif wochentag == 4:
    MODUS = 'wochenend_vorbereitung'
elif wochentag in [5, 6]:
    MODUS = 'spieltag'
else:
    MODUS = 'normal'

print(f"Modus: {MODUS.upper()}")
print()

# ============================================================
# SCHRITT 1: VERLETZUNGEN & SPERREN LADEN
# ============================================================

def lade_verletzungen():
    """Lädt aktuelle Verletzungen und Sperren von API-Football."""
    print("Lade Verletzungen & Sperren...")

    if API_FOOTBALL_KEY == 'DEIN_API_FOOTBALL_KEY':
        print("  ⚠ API-Football Key nicht eingetragen – überspringe")
        return {}

    verletzungen = {}
    anfragen_limit = 20 if wochentag == 4 else 10  # Fr mehr Anfragen

    for liga, liga_id in list(LIGA_IDS.items())[:3]:  # Max 3 Ligen pro Tag
        data = api_football('injuries', {
            'league': liga_id,
            'season': 2024,
        })

        if not data:
            continue

        liga_verletzt = []
        for eintrag in data:
            spieler = eintrag.get('player', {})
            team = eintrag.get('team', {})
            liga_verletzt.append({
                'team': team.get('name', ''),
                'team_id': team.get('id'),
                'spieler': spieler.get('name', ''),
                'grund': spieler.get('reason', ''),
                'typ': spieler.get('type', ''),
            })

        verletzungen[liga] = liga_verletzt
        print(f"  {liga}: {len(liga_verletzt)} Ausfälle")

    return verletzungen

def verletzungs_faktor(team_name, liga, verletzungen_dict):
    """
    Berechnet einen Faktor 0-1 basierend auf Verletzungen.
    0.0 = sehr viele Ausfälle (schlecht für Over-Wette)
    1.0 = keine Ausfälle
    """
    if liga not in verletzungen_dict:
        return 0.5  # Unbekannt = neutral

    team_v = [v for v in verletzungen_dict[liga]
              if team_name.lower()[:5] in v['team'].lower()[:5]
              or v['team'].lower()[:5] in team_name.lower()[:5]]

    anzahl = len(team_v)
    if anzahl == 0: return 1.0
    elif anzahl <= 2: return 0.85
    elif anzahl <= 4: return 0.70
    elif anzahl <= 6: return 0.55
    else: return 0.40

# ============================================================
# SCHRITT 2: OFFENE WETTEN AUSFÜLLEN
# ============================================================

def fulle_offene_wetten():
    print("Fülle offene Wetten mit Ergebnissen...")
    offene = sb_get('wetten', 'status=eq.offen&select=*')

    if not offene:
        print("  Keine offenen Wetten.")
        return 0

    ausgefuellt = 0
    for liga, url in LIGA_URLS.items():
        liga_offen = [w for w in offene if w.get('liga') == liga]
        if not liga_offen:
            continue

        df = lade_csv(url)
        if df is None or 'FTHG' not in df.columns:
            continue

        gespielt = df[df['FTHG'].notna()].copy()
        gespielt['FTHG'] = pd.to_numeric(gespielt['FTHG'], errors='coerce')
        gespielt['FTAG'] = pd.to_numeric(gespielt['FTAG'], errors='coerce')
        gespielt = gespielt.dropna(subset=['FTHG', 'FTAG'])

        for wette in liga_offen:
            w_heim = str(wette.get('heim','')).lower().strip()
            w_ausw = str(wette.get('ausw','')).lower().strip()

            for _, spiel in gespielt.iterrows():
                s_heim = str(spiel['HomeTeam']).lower().strip()
                s_ausw = str(spiel['AwayTeam']).lower().strip()

                h_match = w_heim[:5] in s_heim or s_heim[:5] in w_heim
                a_match = w_ausw[:5] in s_ausw or s_ausw[:5] in w_ausw

                if h_match and a_match:
                    th, ta = int(spiel['FTHG']), int(spiel['FTAG'])
                    ueber = (th + ta) > 2
                    gew = ueber if wette['richtung'] == 'ueber' else not ueber
                    status = 'gewonnen' if gew else 'verloren'
                    quote = float(wette.get('quote', 1.80))
                    einsatz = float(wette.get('einsatz', 1.50))
                    gewinn = round(einsatz*(quote-1), 2) if gew else round(-einsatz, 2)

                    if sb_update('wetten', wette['id'], {
                        'status': status, 'tore_heim': th,
                        'tore_ausw': ta, 'gewinn': gewinn
                    }):
                        symbol = "✓" if gew else "✗"
                        print(f"  {symbol} {wette['heim']} vs {wette['ausw']}: {th}:{ta} → {status}")
                        ausgefuellt += 1
                    break

    print(f"  {ausgefuellt} Wetten ausgefüllt")
    return ausgefuellt

# ============================================================
# SCHRITT 3: EMPFEHLUNGEN BERECHNEN (mit Verletzungscheck)
# ============================================================

def berechne_empfehlungen(verletzungen_dict={}):
    """Berechnet Empfehlungen mit Verletzungsfaktor."""
    config = lade_config()
    strategien = config.get('strategien', {})

    wetten_alle = sb_get('wetten', 'select=*')
    abg = [w for w in wetten_alle if w.get('status') in ['gewonnen','verloren']]
    bankroll = STARTKAPITAL + sum(float(w.get('gewinn',0)) for w in abg)

    empfehlungen = []

    for liga, url in LIGA_URLS.items():
        strat = strategien.get(liga, {})
        if not strat.get('aktiv', True):
            print(f"  {liga}: PAUSIERT – überspringe")
            continue

        df = lade_csv(url)
        if df is None:
            continue

        min_impl = strat.get('min_impl_unter', 0.50)
        erlaubte_tage = strat.get('wochentage', [0, 6])
        richtung = strat.get('richtung', 'ueber')

        if 'Avg<2.5' not in df.columns:
            continue

        zukuenftig = df[df['FTHG'].isna()].copy() if 'FTHG' in df.columns else df.copy()
        zukuenftig = zukuenftig.dropna(subset=['Avg<2.5','Avg>2.5','Date'])

        for _, spiel in zukuenftig.iterrows():
            try:
                qU = float(spiel['Avg<2.5'])
                qO = float(spiel['Avg>2.5'])
                if qU <= 1 or qO <= 1:
                    continue

                impl = (1/qU) / (1/qU + 1/qO)
                datum = pd.to_datetime(spiel['Date'], dayfirst=True)
                tag = datum.weekday()

                if impl < min_impl:
                    continue
                if tag not in erlaubte_tage:
                    continue

                # Verletzungsfaktor einberechnen
                v_heim = verletzungs_faktor(str(spiel['HomeTeam']), liga, verletzungen_dict)
                v_ausw = verletzungs_faktor(str(spiel['AwayTeam']), liga, verletzungen_dict)
                v_faktor = (v_heim + v_ausw) / 2

                # Angepasste Wahrscheinlichkeit
                impl_angepasst = impl * v_faktor if richtung == 'ueber' else impl / max(v_faktor, 0.1)

                quote = qO if richtung == 'ueber' else qU
                edge_basis = (impl - min_impl) * 100
                edge_angepasst = (impl_angepasst - min_impl) * 100

                einsatz = round(bankroll * 0.03, 2)

                empfehlungen.append({
                    'liga': liga,
                    'heim': str(spiel['HomeTeam']),
                    'ausw': str(spiel['AwayTeam']),
                    'datum': datum.strftime('%d.%m.%Y'),
                    'wochentag': WOCHENTAGE_DE[tag],
                    'richtung': richtung,
                    'quote': quote,
                    'impl_unter': round(impl, 3),
                    'impl_angepasst': round(impl_angepasst, 3),
                    'edge': round(edge_basis, 1),
                    'edge_angepasst': round(edge_angepasst, 1),
                    'verletzungen_heim': round(v_heim, 2),
                    'verletzungen_ausw': round(v_ausw, 2),
                    'einsatz': einsatz,
                    'signal_staerke': 'stark' if edge_angepasst > 5 else 'mittel',
                })

            except:
                continue

    return sorted(empfehlungen, key=lambda x: x['datum'])

# ============================================================
# HAUPTLOGIK NACH MODUS
# ============================================================

verletzungen = {}

if MODUS == 'vollanalyse':
    print("━"*65)
    print("MONTAG: VOLLANALYSE")
    print("━"*65)

    # 1. Offene Wetten auffüllen
    fulle_offene_wetten()

    # 2. Verletzungen laden
    verletzungen = lade_verletzungen()

    # 3. Performance analysieren & Strategie anpassen
    print()
    print("Analysiere Performance & passe Strategie an...")
    wetten_alle = sb_get('wetten', 'select=*')
    abg = [w for w in wetten_alle if w.get('status') in ['gewonnen','verloren']]

    config = lade_config()
    if not config.get('strategien'):
        print("  Keine Config gefunden – nutze Standard")
        from copy import deepcopy
        config = {
            'version': 1,
            'strategien': {
                'Premier_League': {'aktiv': True, 'richtung': 'ueber', 'min_impl_unter': 0.50, 'wochentage': [0], 'roi_live': None, 'wetten_live': 0},
                '2_Bundesliga':   {'aktiv': True, 'richtung': 'ueber', 'min_impl_unter': 0.50, 'wochentage': [0,6], 'roi_live': None, 'wetten_live': 0},
                'Scottish_Prem':  {'aktiv': True, 'richtung': 'ueber', 'min_impl_unter': 0.50, 'wochentage': [0], 'roi_live': None, 'wetten_live': 0},
                'Championship':   {'aktiv': True, 'richtung': 'ueber', 'min_impl_unter': 0.50, 'wochentage': [0], 'roi_live': None, 'wetten_live': 0},
                'Eredivisie':     {'aktiv': True, 'richtung': 'ueber', 'min_impl_unter': 0.55, 'wochentage': list(range(7)), 'roi_live': None, 'wetten_live': 0},
            },
            'globale_einstellungen': {'einsatz_pct': 0.03, 'min_wetten_fuer_anpassung': 20, 'roi_warnschwelle': -10.0, 'roi_pausenschwelle': -20.0}
        }

    aenderungen = []
    for liga, strat in config['strategien'].items():
        liga_w = [w for w in abg if w.get('liga') == liga]
        if len(liga_w) < 5:
            continue

        g = sum(float(w.get('gewinn',0)) for w in liga_w)
        avg_e = np.mean([float(w.get('einsatz',1.5)) for w in liga_w])
        roi = g / (len(liga_w) * avg_e) * 100
        strat['roi_live'] = round(roi, 1)
        strat['wetten_live'] = len(liga_w)

        MIN_W = config['globale_einstellungen']['min_wetten_fuer_anpassung']
        if len(liga_w) >= MIN_W:
            PAUSE = config['globale_einstellungen']['roi_pausenschwelle']
            WARN  = config['globale_einstellungen']['roi_warnschwelle']
            if roi < PAUSE and strat['aktiv']:
                strat['aktiv'] = False
                aenderungen.append(f"⛔ {liga} DEAKTIVIERT (ROI {roi:+.1f}%)")
            elif roi < WARN:
                old = strat['min_impl_unter']
                strat['min_impl_unter'] = min(old + 0.02, 0.65)
                aenderungen.append(f"⚠ {liga} Schwelle {old:.2f}→{strat['min_impl_unter']:.2f} (ROI {roi:+.1f}%)")
            elif roi > 15 and strat['min_impl_unter'] > 0.48:
                old = strat['min_impl_unter']
                strat['min_impl_unter'] = max(old - 0.01, 0.48)
                aenderungen.append(f"✓ {liga} Schwelle {old:.2f}→{strat['min_impl_unter']:.2f} (ROI {roi:+.1f}%)")

        print(f"  {liga}: ROI={roi:+.1f}% | {len(liga_w)} Wetten | {'AKTIV' if strat['aktiv'] else 'PAUSIERT'}")

    speichere_config(config)

    if aenderungen:
        print()
        print("  STRATEGIE-ÄNDERUNGEN:")
        for a in aenderungen: print(f"  {a}")

    # 4. Wochenvorschau
    print()
    print("Wochenvorschau:")
    empf = berechne_empfehlungen(verletzungen)
    if empf:
        for e in empf[:5]:
            v_info = f" | Verletzte: H={e['verletzungen_heim']:.0%} A={e['verletzungen_ausw']:.0%}"
            print(f"  {e['datum']} ({e['wochentag']}) | {e['liga']} | {e['heim']} vs {e['ausw']}")
            print(f"    {e['richtung'].upper()} @ {e['quote']:.2f} | Edge: {e['edge']:+.1f}% (adj: {e['edge_angepasst']:+.1f}%){v_info}")
    else:
        print("  Keine Empfehlungen gefunden.")

elif MODUS == 'midweek':
    print("━"*65)
    print(f"{wochentag_name.upper()}: MIDWEEK-CHECK")
    print("━"*65)

    fulle_offene_wetten()
    verletzungen = lade_verletzungen()

    empf = berechne_empfehlungen(verletzungen)
    midweek = [e for e in empf if pd.to_datetime(e['datum'], dayfirst=True).weekday() in [1,2,3]]

    if midweek:
        print(f"\n  {len(midweek)} Midweek-Empfehlung(en):")
        for e in midweek:
            print(f"  {e['datum']} | {e['liga']} | {e['heim']} vs {e['ausw']}")
            print(f"  → {e['richtung'].upper()} @ {e['quote']:.2f} | Edge {e['edge_angepasst']:+.1f}%")
    else:
        print("  Keine Midweek-Empfehlungen.")

elif MODUS == 'wochenend_vorbereitung':
    print("━"*65)
    print("FREITAG: WOCHENEND-VORBEREITUNG")
    print("━"*65)

    fulle_offene_wetten()
    verletzungen = lade_verletzungen()
    empf = berechne_empfehlungen(verletzungen)
    wochenend = [e for e in empf if pd.to_datetime(e['datum'], dayfirst=True).weekday() in [5,6]]

    print(f"\n  {len(wochenend)} Wochenend-Empfehlung(en):")
    print()
    for e in wochenend:
        print(f"  {'★' if e['signal_staerke']=='stark' else '·'} {e['datum']} ({e['wochentag']}) | {e['liga']}")
        print(f"    {e['heim']} vs {e['ausw']}")
        print(f"    {e['richtung'].upper()} 2.5 @ {e['quote']:.2f} | Edge: {e['edge_angepasst']:+.1f}% | Einsatz: {e['einsatz']:.2f}€")
        v_h = "⚠ Viele Ausfälle" if e['verletzungen_heim'] < 0.6 else "OK"
        v_a = "⚠ Viele Ausfälle" if e['verletzungen_ausw'] < 0.6 else "OK"
        print(f"    Heim-Kader: {v_h} | Auswärts-Kader: {v_a}")
        print()

elif MODUS == 'spieltag':
    print("━"*65)
    print(f"{wochentag_name.upper()}: SPIELTAG-CHECK")
    print("━"*65)

    # Offene Wetten sofort prüfen
    fulle_offene_wetten()

    # Verletzungen nochmal prüfen (kurzfristige Ausfälle)
    verletzungen = lade_verletzungen()

    # Nur heutige Spiele
    empf = berechne_empfehlungen(verletzungen)
    heute_str = now.strftime('%d.%m.%Y')
    heute_empf = [e for e in empf if e['datum'] == heute_str]

    if heute_empf:
        print(f"\n  {len(heute_empf)} Spiel(e) HEUTE mit Signal:")
        for e in heute_empf:
            print(f"  ★ {e['heim']} vs {e['ausw']} ({e['liga']})")
            print(f"    {e['richtung'].upper()} @ {e['quote']:.2f} | Edge {e['edge_angepasst']:+.1f}%")
            if e['verletzungen_heim'] < 0.6:
                print(f"    ⚠ Heimteam hat viele Ausfälle!")
            if e['verletzungen_ausw'] < 0.6:
                print(f"    ⚠ Auswärtsteam hat viele Ausfälle!")
    else:
        print("  Keine Signale für heute.")

# ============================================================
# ZUSAMMENFASSUNG
# ============================================================

print()
print("="*65)
wetten_alle = sb_get('wetten', 'select=*')
abg = [w for w in wetten_alle if w.get('status') in ['gewonnen','verloren']]
offen = [w for w in wetten_alle if w.get('status') == 'offen']
bankroll = STARTKAPITAL + sum(float(w.get('gewinn',0)) for w in abg)
roi = (bankroll - STARTKAPITAL) / STARTKAPITAL * 100 if abg else 0
hit = sum(1 for w in abg if w['status']=='gewonnen') / len(abg) * 100 if abg else 0

print(f"  Bankroll: {bankroll:.2f}€ | ROI: {roi:+.1f}% | Hit: {hit:.1f}% | Offen: {len(offen)}")
print(f"  Nächstes Update: {(now + timedelta(days=1)).strftime('%d.%m.%Y')} 08:00")
print()

# Nächste Ausführung berechnen
if wochentag == 4:  # Freitag → auch Samstag+Sonntag wichtig
    print("  TIPP: Auch Sa+So um 09:00 ausführen (Spieltag-Check)")
elif wochentag == 0:  # Montag
    print("  VOLLANALYSE abgeschlossen. Nächste: nächsten Montag")

print()
print(">>> Fertig.")
