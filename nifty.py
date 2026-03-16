"""
nifty_report.py
───────────────
1. Reads nifty_data.csv (daily bars, exported from Colab)
2. Aggregates daily → weekly and monthly OHLC
3. Computes 4 range metrics as % of open:
     max_range_pct     = (high - low)   / open * 100
     closing_range_pct = |open - close| / open * 100
     upper_range_pct   = (high - open)  / open * 100
     lower_range_pct   = (open - low)   / open * 100
4. Saves data.json
5. Starts local server and opens browser

Usage:
    python nifty_report.py
"""

import json, math, time, threading, webbrowser, http.server, socketserver, os
import pandas as pd
from pathlib import Path

CSV_FILE = "nifty_data.csv"
OUTPUT   = "data.json"
PORT     = 8000

METRICS = ['max_range_pct', 'closing_range_pct', 'upper_range_pct', 'lower_range_pct']

def clean(obj):
    if isinstance(obj, list): return [clean(i) for i in obj]
    if isinstance(obj, dict): return {k: clean(v) for k, v in obj.items()}
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)): return None
    return obj

def load_data():
    p = Path(CSV_FILE)
    if not p.exists():
        raise FileNotFoundError(
            f"'{CSV_FILE}' not found.\n"
            f"Run the Colab notebook first and place the CSV in:\n{Path.cwd()}"
        )
    print(f"Reading {CSV_FILE}...")
    df = pd.read_csv(p)
    df['date'] = pd.to_datetime(df['date'])
    df = df.dropna(subset=['open','high','low','close'])
    df = df[(df['high'] > df['low']) & (df['open'] > 0)]
    df = df.sort_values('date').reset_index(drop=True)
    print(f"  Loaded {len(df)} daily rows  ({df['date'].min().date()} → {df['date'].max().date()})")
    return df

def aggregate(df, freq):
    """
    Aggregate daily OHLC to weekly (freq='W-FRI') or monthly (freq='MS').
    Weekly : open=Mon open, high=week high, low=week low, close=Fri close
    Monthly: open=1st day open, high=month high, low=month low, close=last day close
    """
    df = df.set_index('date')
    agg = df.groupby(pd.Grouper(freq=freq)).agg(
        open  = ('open',  'first'),
        high  = ('high',  'max'),
        low   = ('low',   'min'),
        close = ('close', 'last'),
    ).dropna().reset_index()
    agg = agg[(agg['high'] > agg['low']) & (agg['open'] > 0)]
    return agg

def compute_pct(df):
    df = df.copy()
    df['max_range_pct']     = ((df['high'] - df['low'])           / df['open'] * 100).round(3)
    df['closing_range_pct'] = ((df['open'] - df['close']).abs()   / df['open'] * 100).round(3)
    df['upper_range_pct']   = ((df['high'] - df['open'])          / df['open'] * 100).round(3)
    df['lower_range_pct']   = ((df['open'] - df['low'])           / df['open'] * 100).round(3)
    return df

def yearly_stats(df, date_col):
    """
    For each year:
      avg  = mean of all candle ranges
      high = max single candle range (worst week / worst month)
      low  = min single candle range (calmest week / month)
    """
    df = df.copy()
    df['year'] = pd.to_datetime(df[date_col]).dt.year
    rows = []
    for year, grp in df.groupby('year'):
        row = {'year': int(year)}
        for m in METRICS:
            row[m + '_avg']  = round(float(grp[m].mean()), 3)
            row[m + '_high'] = round(float(grp[m].max()),  3)
            row[m + '_low']  = round(float(grp[m].min()),  3)
        rows.append(row)
    return rows

def period_series(df, date_col, label_fmt):
    """
    Return every period row with its label — used for the
    monthly dropdown where user selects how far back to look.
    """
    df = df.copy()
    df['label'] = pd.to_datetime(df[date_col]).dt.strftime(label_fmt)
    df['ym']    = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m')
    cols = ['ym', 'label'] + METRICS
    return df[cols].to_dict(orient='records')

def build_payload(daily):
    weekly  = compute_pct(aggregate(daily, 'W-FRI'))
    monthly = compute_pct(aggregate(daily, 'MS'))

    return {
        'meta': {
            'symbol':      'NIFTY',
            'daily_rows':  int(len(daily)),
            'from':        str(daily['date'].min().date()),
            'to':          str(daily['date'].max().date()),
            'total_years': int((daily['date'].max() - daily['date'].min()).days // 365),
        },
        # Yearly averages (for the yearly section)
        'weekly_yearly':  yearly_stats(weekly,  'date'),
        'monthly_yearly': yearly_stats(monthly, 'date'),

        # Full period series (for dropdown-filtered section)
        'weekly_series':  period_series(weekly,  'date', '%d %b %y'),
        'monthly_series': period_series(monthly, 'date', '%b %Y'),
    }

def start_server(port):
    os.chdir(Path(__file__).parent)
    handler = http.server.SimpleHTTPRequestHandler
    handler.log_message = lambda *a: None
    with socketserver.TCPServer(('', port), handler) as httpd:
        print(f"  Dashboard → http://localhost:{port}")
        print("  Press Ctrl+C to stop.\n")
        httpd.serve_forever()

if __name__ == '__main__':
    daily   = load_data()
    payload = build_payload(daily)

    out = Path(OUTPUT)
    out.write_text(json.dumps(clean(payload), indent=2), encoding='utf-8')
    print(f"\nSaved → {out.resolve()}")
    print(f"  Weekly yearly rows  : {len(payload['weekly_yearly'])}")
    print(f"  Monthly yearly rows : {len(payload['monthly_yearly'])}")
    print(f"  Weekly series rows  : {len(payload['weekly_series'])}")
    print(f"  Monthly series rows : {len(payload['monthly_series'])}")

    t = threading.Thread(target=start_server, args=(PORT,), daemon=True)
    t.start()
    time.sleep(1)
    webbrowser.open(f'http://localhost:{PORT}')

    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print('\nServer stopped.')