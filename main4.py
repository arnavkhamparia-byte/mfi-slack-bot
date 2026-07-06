import os
import psycopg2
import urllib.request
import urllib.error
import json
import time
from datetime import datetime
import pytz

IST = pytz.timezone("Asia/Kolkata")

DB_HOST = "otolmsstagedbinstance.cttxlpcdrmsq.ap-south-1.rds.amazonaws.com"
DB_PORT = 5432
DB_NAME = "seeds_fincap"
DB_USER = "readonly"
DB_PASS = "readonly"

WEBHOOK_URL = os.environ["WEBHOOK_URL_4"]

HOUR_SLOTS  = [9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
HOUR_LABELS = {
    9:  "9-10AM",  10: "10-11AM", 11: "11-12PM", 12: "12-1PM",
    13: "1-2PM",   14: "2-3PM",   15: "3-4PM",
    16: "4-5PM",   17: "5-6PM",   18: "6-7PM",   19: "7-8PM",
}
HOUR_SHORT = {
    9:  "9AM",  10: "10AM", 11: "11AM", 12: "12PM",
    13: "1PM",  14: "2PM",  15: "3PM",
    16: "4PM",  17: "5PM",  18: "6PM",  19: "7PM",
}

# All queries are scoped to AI calls only (table also holds Manual Call rows)
AI_FILTER = "channel = 'AI Call'"


def is_within_business_hours():
    now_ist = datetime.now(IST)
    return 9 <= now_ist.hour < 20


def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


# ── Section 1: Summary Stats ──────────────────────────────────────

def fetch_section1(cur, today_ist):
    cur.execute(f"""
        SELECT
            COUNT(*) FILTER (
                WHERE status = 'done'
                AND (outcome <> 'RescheduledToNextDay' OR outcome IS NULL)
                AND DATE(processed_at AT TIME ZONE 'Asia/Kolkata') = %s
            ) AS done_today,
            COUNT(*) FILTER (
                WHERE status = 'enqueued'
                AND DATE(eta AT TIME ZONE 'Asia/Kolkata') = %s
            ) AS enqueued_today,
            COUNT(*) FILTER (
                WHERE status = 'failed'
                AND DATE(modified AT TIME ZONE 'Asia/Kolkata') = %s
            ) AS failed_today,
            COUNT(*) FILTER (
                WHERE status = 'skipped'
                AND DATE(modified AT TIME ZONE 'Asia/Kolkata') = %s
            ) AS skipped_today,
            COUNT(*) FILTER (
                WHERE status = 'done'
                AND task_status = 'Connected'
                AND (outcome <> 'RescheduledToNextDay' OR outcome IS NULL)
                AND DATE(processed_at AT TIME ZONE 'Asia/Kolkata') = %s
            ) AS connected_today,
            COUNT(*) FILTER (
                WHERE status = 'done'
                AND task_status = 'Not Connected'
                AND (outcome <> 'RescheduledToNextDay' OR outcome IS NULL)
                AND DATE(processed_at AT TIME ZONE 'Asia/Kolkata') = %s
            ) AS not_connected_today,
            COUNT(*) FILTER (
                WHERE status = 'done'
                AND task_status NOT IN ('Connected', 'Not Connected')
                AND task_status IS NOT NULL AND task_status <> ''
                AND (outcome <> 'RescheduledToNextDay' OR outcome IS NULL)
                AND DATE(processed_at AT TIME ZONE 'Asia/Kolkata') = %s
            ) AS other_today,
            COUNT(*) FILTER (
                WHERE status = 'done'
                AND (task_status IS NULL OR task_status = '')
                AND (outcome <> 'RescheduledToNextDay' OR outcome IS NULL)
                AND DATE(processed_at AT TIME ZONE 'Asia/Kolkata') = %s
            ) AS null_task_status_today,
            COUNT(*) FILTER (
                WHERE status = 'done'
                AND ptp IS NOT NULL
                AND DATE(processed_at AT TIME ZONE 'Asia/Kolkata') = %s
            ) AS ptp_today,
            COALESCE(ROUND(AVG(call_duration) FILTER (
                WHERE status = 'done'
                AND task_status = 'Connected'
                AND DATE(processed_at AT TIME ZONE 'Asia/Kolkata') = %s
            )), 0) AS avg_duration,
            COUNT(*) FILTER (
                WHERE status = 'enqueued'
                AND eta < now() - interval '1 day'
            ) AS stale_enqueued
        FROM activity_taskactivity
        WHERE {AI_FILTER}
    """, (today_ist,) * 10)
    row = cur.fetchone()
    return {
        'done':          row[0],
        'enqueued':      row[1],
        'failed':        row[2],
        'skipped':       row[3],
        'connected':     row[4],
        'not_connected': row[5],
        'other':         row[6],
        'null_status':   row[7],
        'ptp':           row[8],
        'avg_duration':  row[9],
        'stale':         row[10],
    }


# ── Section 2: AI Call Quality (Hourly) ──────────────────────────

def fetch_section2(cur, today_ist):
    cur.execute(f"""
        SELECT EXTRACT(HOUR FROM eta AT TIME ZONE 'Asia/Kolkata')::int, COUNT(*)
        FROM activity_taskactivity
        WHERE {AI_FILTER}
          AND status = 'enqueued'
          AND DATE(eta AT TIME ZONE 'Asia/Kolkata') = %s
          AND EXTRACT(HOUR FROM eta AT TIME ZONE 'Asia/Kolkata') = ANY(%s)
        GROUP BY 1
    """, (today_ist, HOUR_SLOTS))
    enqueued = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute(f"""
        SELECT
            EXTRACT(HOUR FROM processed_at AT TIME ZONE 'Asia/Kolkata')::int,
            COUNT(*) FILTER (WHERE outcome <> 'RescheduledToNextDay' OR outcome IS NULL),
            COUNT(*) FILTER (WHERE task_status = 'Connected'),
            COUNT(*) FILTER (WHERE task_status = 'Connected' AND call_duration >= 20),
            COUNT(*) FILTER (WHERE task_status = 'Connected' AND call_duration < 20),
            COUNT(*) FILTER (WHERE task_status = 'Not Connected'),
            COUNT(*) FILTER (WHERE task_status NOT IN ('Connected', 'Not Connected')
                AND task_status IS NOT NULL AND task_status <> ''
                AND (outcome <> 'RescheduledToNextDay' OR outcome IS NULL)),
            COUNT(*) FILTER (WHERE (task_status IS NULL OR task_status = '')
                AND (outcome <> 'RescheduledToNextDay' OR outcome IS NULL))
        FROM activity_taskactivity
        WHERE {AI_FILTER}
          AND status = 'done'
          AND DATE(processed_at AT TIME ZONE 'Asia/Kolkata') = %s
          AND EXTRACT(HOUR FROM processed_at AT TIME ZONE 'Asia/Kolkata') = ANY(%s)
        GROUP BY 1
    """, (today_ist, HOUR_SLOTS))
    quality = {r[0]: r[1:] for r in cur.fetchall()}

    return {'enqueued': enqueued, 'quality': quality}


# ── Section 3: Disposition Breakdown (Hourly) ────────────────────

def fetch_section3(cur, today_ist):
    cur.execute(f"""
        SELECT
            EXTRACT(HOUR FROM processed_at AT TIME ZONE 'Asia/Kolkata')::int,
            disposition,
            COUNT(*)
        FROM activity_taskactivity
        WHERE {AI_FILTER}
          AND status = 'done'
          AND DATE(processed_at AT TIME ZONE 'Asia/Kolkata') = %s
          AND EXTRACT(HOUR FROM processed_at AT TIME ZONE 'Asia/Kolkata') = ANY(%s)
          AND disposition IS NOT NULL AND disposition <> ''
        GROUP BY 1, 2
    """, (today_ist, HOUR_SLOTS))
    data = {}
    for hour, disposition, count in cur.fetchall():
        data.setdefault(disposition, {})[int(hour)] = count
    return data


# ── Formatters ────────────────────────────────────────────────────

def format_section1(data, now_ist):
    lw, cw = 28, 10
    header = f"{'Metric':<{lw}}{'Count':>{cw}}"
    sep    = "-" * len(header)
    rate = f"{data['connected'] / data['done'] * 100:.1f}%" if data['done'] else "-"
    rows = [
        ("Done Today",             f"{data['done']:,}"),
        ("Enqueued Today",         f"{data['enqueued']:,}"),
        ("Failed Today",           f"{data['failed']:,}"),
        ("Skipped Today",          f"{data['skipped']:,}"),
        (" ", " "),
        ("Connected",              f"{data['connected']:,}"),
        ("Not Connected",          f"{data['not_connected']:,}"),
        ("Other (RNR/busy/...)",   f"{data['other']:,}"),
        ("Null-no task_status",    f"{data['null_status']:,}"),
        (" ", " "),
        ("Connect Rate",           rate),
        ("PTPs Captured",          f"{data['ptp']:,}"),
        ("Avg Duration-Connected", f"{data['avg_duration']:.0f}s"),
        ("Stale Enqueued (>1d)",   f"{data['stale']:,}"),
    ]
    table = "\n".join([header, sep] + [f"{l:<{lw}}{v:>{cw}}" for l, v in rows])
    return (
        f":bar_chart: *Seeds Fincap Dashboard — Summary Stats* | "
        f"{now_ist.strftime('%d %b %Y, %I:%M %p')} IST\n"
        f"```\n{table}\n```"
    )


def format_section2(data, now_ist):
    enqueued = data['enqueued']
    quality  = data['quality']

    mw, cw = 22, 6
    short  = [HOUR_SHORT[h] for h in HOUR_SLOTS]
    header = f"{'Metric':<{mw}}" + "".join(f"{l:>{cw}}" for l in short)
    sep    = "-" * len(header)

    metrics = [
        ("Enqueued",            None),
        ("Connected",           1),
        (">= 20s",              2),
        ("< 20s",               3),
        ("Not Connected",       4),
        ("Other",               5),
        ("Null-no task_status", 6),
    ]
    empty = (0,) * 7
    rows = []
    for label, idx in metrics:
        if idx is None:
            row = f"{label:<{mw}}" + "".join(f"{enqueued.get(h, 0):>{cw}}" for h in HOUR_SLOTS)
        else:
            row = f"{label:<{mw}}" + "".join(f"{quality.get(h, empty)[idx]:>{cw}}" for h in HOUR_SLOTS)
        rows.append(row)

    total = f"{'TOTAL':<{mw}}" + "".join(f"{quality.get(h, empty)[0]:>{cw}}" for h in HOUR_SLOTS)
    conn_rate = f"{'Connect Rate %':<{mw}}"
    for h in HOUR_SLOTS:
        q = quality.get(h, empty)
        conn_rate += f"{(f'{q[1] / q[0] * 100:.0f}%' if q[0] else '-'):>{cw}}"
    table = "\n".join([header, sep] + rows + [sep, total, conn_rate])

    return (
        f":telephone_receiver: *Seeds Fincap Dashboard — AI Call Quality (Hourly)* | "
        f"{now_ist.strftime('%d %b %Y, %I:%M %p')} IST\n"
        f"```\n{table}\n```"
    )


def format_section3(data, now_ist):
    lw, cw = 32, 6
    short  = [HOUR_SHORT[h] for h in HOUR_SLOTS]
    header = f"{'Disposition':<{lw}}" + "".join(f"{l:>{cw}}" for l in short)
    sep    = "-" * len(header)

    # Dynamic: every disposition seen today, sorted by daily total (desc)
    disps = sorted(data, key=lambda d: -sum(data[d].values()))
    rows = []
    for disp in disps:
        hour_data = data[disp]
        label = disp if len(disp) <= lw else disp[:lw - 2] + ".."
        row = f"{label:<{lw}}" + "".join(f"{hour_data.get(h, 0):>{cw}}" for h in HOUR_SLOTS)
        rows.append(row)
    if not rows:
        rows = ["(no dispositions recorded yet today)"]

    table = "\n".join([header, sep] + rows)
    return (
        f":memo: *Seeds Fincap Dashboard — Disposition Breakdown (Hourly)* | "
        f"{now_ist.strftime('%d %b %Y, %I:%M %p')} IST\n"
        f"```\n{table}\n```"
    )


# ── Slack ─────────────────────────────────────────────────────────

def send_to_slack(message, max_retries=3):
    payload = json.dumps({"text": message}).encode()
    for attempt in range(max_retries):
        req = urllib.request.Request(
            WEBHOOK_URL, data=payload,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        try:
            resp = urllib.request.urlopen(req)
            print(f"Sent to Slack: {resp.status}")
            return
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < max_retries - 1:
                retry_after = int(e.headers.get("Retry-After", 1))
                print(f"Rate limited by Slack (429), retrying in {retry_after}s...")
                time.sleep(retry_after)
                continue
            raise


# ── Main ──────────────────────────────────────────────────────────

def main():
    if not is_within_business_hours():
        print("Outside business hours (9AM–8PM IST). Skipping.")
        return

    now_ist   = datetime.now(IST)
    today_ist = now_ist.date()
    print(f"Fetching Seeds Fincap dashboard data for {today_ist}...")

    conn = get_connection()
    cur  = conn.cursor()

    s1 = fetch_section1(cur, today_ist)
    s2 = fetch_section2(cur, today_ist)
    s3 = fetch_section3(cur, today_ist)

    cur.close()
    conn.close()

    for msg in [
        format_section1(s1, now_ist),
        format_section2(s2, now_ist),
        format_section3(s3, now_ist),
    ]:
        send_to_slack(msg)

    print("Done.")


if __name__ == "__main__":
    main()
