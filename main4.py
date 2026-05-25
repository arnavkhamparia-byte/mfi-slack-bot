import os
import psycopg2
import urllib.request
import json
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

DISPOSITIONS = [
    "Agree To Pay", "Agree To Senior Manager Call",
    "busy", "Call Back Requested", "Call Hang Up",
    "Call Hang Up->LESS THEN 20sec", "Connected", "Dispute",
    "Failed", "Financial Hardship", "Information Conveyed",
    "no-answer", "not-connected", "Refuse To Pay",
    "RescheduledToNextDay", "Unclear", "Wrong Number",
]


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
    cur.execute("""
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
                WHERE status = 'done'
                AND task_status = 'Connected'
                AND DATE(processed_at AT TIME ZONE 'Asia/Kolkata') = %s
            ) AS connected_today,
            COUNT(*) FILTER (
                WHERE status = 'done'
                AND task_status = 'Not Connected'
                AND DATE(processed_at AT TIME ZONE 'Asia/Kolkata') = %s
            ) AS not_connected_today,
            COUNT(*) FILTER (
                WHERE status = 'done'
                AND (task_status IS NULL OR task_status = '')
                AND (outcome <> 'RescheduledToNextDay' OR outcome IS NULL)
                AND DATE(processed_at AT TIME ZONE 'Asia/Kolkata') = %s
            ) AS null_task_status_today
        FROM activity_taskactivity
    """, (today_ist, today_ist, today_ist, today_ist, today_ist))
    row = cur.fetchone()
    return {
        'done':          row[0],
        'enqueued':      row[1],
        'connected':     row[2],
        'not_connected': row[3],
        'null_status':   row[4],
    }


# ── Section 2: AI Call Quality (Hourly) ──────────────────────────

def fetch_section2(cur, today_ist):
    cur.execute("""
        SELECT EXTRACT(HOUR FROM eta AT TIME ZONE 'Asia/Kolkata')::int, COUNT(*)
        FROM activity_taskactivity
        WHERE status = 'enqueued'
          AND DATE(eta AT TIME ZONE 'Asia/Kolkata') = %s
          AND EXTRACT(HOUR FROM eta AT TIME ZONE 'Asia/Kolkata') = ANY(%s)
        GROUP BY 1
    """, (today_ist, HOUR_SLOTS))
    enqueued = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("""
        SELECT
            EXTRACT(HOUR FROM processed_at AT TIME ZONE 'Asia/Kolkata')::int,
            COUNT(*) FILTER (WHERE outcome <> 'RescheduledToNextDay' OR outcome IS NULL),
            COUNT(*) FILTER (WHERE task_status = 'Connected'),
            COUNT(*) FILTER (WHERE task_status = 'Connected' AND call_duration > 20),
            COUNT(*) FILTER (WHERE task_status = 'Connected' AND call_duration < 20),
            COUNT(*) FILTER (WHERE task_status = 'Not Connected'),
            COUNT(*) FILTER (WHERE (task_status IS NULL OR task_status = '')
                AND (outcome <> 'RescheduledToNextDay' OR outcome IS NULL))
        FROM activity_taskactivity
        WHERE status = 'done'
          AND DATE(processed_at AT TIME ZONE 'Asia/Kolkata') = %s
          AND EXTRACT(HOUR FROM processed_at AT TIME ZONE 'Asia/Kolkata') = ANY(%s)
        GROUP BY 1
    """, (today_ist, HOUR_SLOTS))
    quality = {r[0]: (r[1], r[2], r[3], r[4], r[5], r[6]) for r in cur.fetchall()}

    return {'enqueued': enqueued, 'quality': quality}


# ── Section 3: Disposition Breakdown (Hourly) ────────────────────

def fetch_section3(cur, today_ist):
    cur.execute("""
        SELECT
            EXTRACT(HOUR FROM processed_at AT TIME ZONE 'Asia/Kolkata')::int,
            disposition,
            COUNT(*)
        FROM activity_taskactivity
        WHERE status = 'done'
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
    lw, cw = 25, 10
    header = f"{'Metric':<{lw}}{'Count':>{cw}}"
    sep    = "-" * len(header)
    rows = [
        ("Done Today",           data['done']),
        ("Enqueued Today",       data['enqueued']),
        ("Connected",            data['connected']),
        ("Not Connected",        data['not_connected']),
        ("Null-no task_status",  data['null_status']),
    ]
    table = "\n".join([header, sep] + [f"{l:<{lw}}{v:>{cw},}" for l, v in rows])
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
        ("> 20s",               2),
        ("< 20s",               3),
        ("Not Connected",       4),
        ("Null-no task_status", 5),
    ]
    rows = []
    for label, idx in metrics:
        if idx is None:
            row = f"{label:<{mw}}" + "".join(f"{enqueued.get(h, 0):>{cw}}" for h in HOUR_SLOTS)
        else:
            row = f"{label:<{mw}}" + "".join(f"{quality.get(h, (0,0,0,0,0,0))[idx]:>{cw}}" for h in HOUR_SLOTS)
        rows.append(row)

    total = f"{'TOTAL':<{mw}}" + "".join(f"{quality.get(h, (0,0,0,0,0,0))[0]:>{cw}}" for h in HOUR_SLOTS)
    table = "\n".join([header, sep] + rows + [sep, total])

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

    rows = []
    for disp in DISPOSITIONS:
        hour_data = data.get(disp, {})
        row = f"{disp:<{lw}}" + "".join(f"{hour_data.get(h, 0):>{cw}}" for h in HOUR_SLOTS)
        rows.append(row)

    table = "\n".join([header, sep] + rows)
    return (
        f":memo: *Seeds Fincap Dashboard — Disposition Breakdown (Hourly)* | "
        f"{now_ist.strftime('%d %b %Y, %I:%M %p')} IST\n"
        f"```\n{table}\n```"
    )


# ── Slack ─────────────────────────────────────────────────────────

def send_to_slack(message):
    payload = json.dumps({"text": message}).encode()
    req = urllib.request.Request(
        WEBHOOK_URL, data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    resp = urllib.request.urlopen(req)
    print(f"Sent to Slack: {resp.status}")


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
