import os
import psycopg2
import urllib.request
import json
from datetime import datetime
import pytz

IST = pytz.timezone("Asia/Kolkata")

DB_HOST = "otolmsstagedbinstance.cttxlpcdrmsq.ap-south-1.rds.amazonaws.com"
DB_PORT = 5432
DB_NAME = "fusion_finance_mfi"
DB_USER = "readonly"
DB_PASS = "readonly"

WEBHOOK_URL = os.environ["WEBHOOK_URL_2"]

HOUR_SLOTS  = [9, 10, 11, 13, 14, 15, 16, 17, 18, 19]
HOUR_LABELS = {
    9:  "9-10AM",  10: "10-11AM", 11: "11-12PM",
    13: "1-2PM",   14: "2-3PM",   15: "3-4PM",
    16: "4-5PM",   17: "5-6PM",   18: "6-7PM",  19: "7-8PM",
}
HOUR_SHORT = {
    9:  "9AM",  10: "10AM", 11: "11AM",
    13: "1PM",  14: "2PM",  15: "3PM",
    16: "4PM",  17: "5PM",  18: "6PM",  19: "7PM",
}

DISPOSITIONS = [
    "Agree To Pay", "Agree To Senior Manager Call", "busy", "Busy",
    "Call Back Requested", "Call Hang Up", "Call Hang Up->LESS THEN 20sec",
    "cash pickup", "Connected", "Dispute", "Failed", "Financial Hardship",
    "Follow Up", "Information Conveyed", "Invalid Number", "no-answer",
    "not-connected", "Paid On Call", "Partial Payment", "Payment Claimed",
    "Payment Paid", "Refuse To Pay", "Requested Settlement", "RescheduledToNextDay",
    "Switched Off", "Unacceptable Promise To Pay", "Unclear", "Unpaid",
    "Will Pay", "Wrong Number",
]

FLOWS = ["emi_restart_collection", "long_overdue_recovery", "Post Due", "RNR", "Telecall"]


def is_within_business_hours():
    now_ist = datetime.now(IST)
    return 9 <= now_ist.hour < 20


def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


# ── Section 1: Summary Stats ──────────────────────────────────────────────────

def fetch_section1(cur, today_ist):
    # Total accounts by allocation (live snapshot)
    cur.execute("""
        SELECT account_allocation, COUNT(*)
        FROM account_details
        WHERE account_allocation IN ('System', 'Manual')
        GROUP BY account_allocation
    """)
    total = {r[0]: r[1] for r in cur.fetchall()}

    # Calls done today (count + unique accounts)
    cur.execute("""
        SELECT ad.account_allocation, COUNT(*), COUNT(DISTINCT ata.account_id)
        FROM activity_taskactivity ata
        JOIN account_details ad ON ata.account_id = ad.id
        WHERE ata.status = 'done'
          AND (ata.outcome <> 'RescheduledToNextDay' OR ata.outcome IS NULL)
          AND DATE(ata.processed_at AT TIME ZONE 'Asia/Kolkata') = %s
        GROUP BY ad.account_allocation
    """, (today_ist,))
    done = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

    # Calls enqueued for today (count + unique accounts)
    cur.execute("""
        SELECT ad.account_allocation, COUNT(*), COUNT(DISTINCT ata.account_id)
        FROM activity_taskactivity ata
        JOIN account_details ad ON ata.account_id = ad.id
        WHERE ata.status = 'enqueued'
          AND DATE(ata.eta AT TIME ZONE 'Asia/Kolkata') = %s
        GROUP BY ad.account_allocation
    """, (today_ist,))
    enqueued = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

    # Accounts enqueued today with zero done calls today
    cur.execute("""
        SELECT ad.account_allocation, COUNT(DISTINCT ata.account_id)
        FROM activity_taskactivity ata
        JOIN account_details ad ON ata.account_id = ad.id
        WHERE ata.status = 'enqueued'
          AND DATE(ata.eta AT TIME ZONE 'Asia/Kolkata') = %s
          AND ata.account_id NOT IN (
              SELECT DISTINCT account_id
              FROM activity_taskactivity
              WHERE status = 'done'
                AND (outcome <> 'RescheduledToNextDay' OR outcome IS NULL)
                AND DATE(processed_at AT TIME ZONE 'Asia/Kolkata') = %s
          )
        GROUP BY ad.account_allocation
    """, (today_ist, today_ist))
    no_activity = {r[0]: r[1] for r in cur.fetchall()}

    return {
        'total':       total,
        'done':        done,
        'enqueued':    enqueued,
        'no_activity': no_activity,
    }


# ── Section 2: AI Call Queue vs Processed (Hourly) ───────────────────────────

def fetch_section2(cur, today_ist):
    # Column 1: Enqueued per slot (eta falls in slot, today)
    cur.execute("""
        SELECT EXTRACT(HOUR FROM ata.eta AT TIME ZONE 'Asia/Kolkata')::int, COUNT(*)
        FROM activity_taskactivity ata
        JOIN account_details ad ON ata.account_id = ad.id
        WHERE ata.status = 'enqueued'
          AND ad.account_allocation = 'System'
          AND DATE(ata.eta AT TIME ZONE 'Asia/Kolkata') = %s
          AND EXTRACT(HOUR FROM ata.eta AT TIME ZONE 'Asia/Kolkata') = ANY(%s)
        GROUP BY 1
    """, (today_ist, HOUR_SLOTS))
    enqueued = {r[0]: r[1] for r in cur.fetchall()}

    # Column 2: Scheduled for slot AND executed in same slot today
    cur.execute("""
        SELECT EXTRACT(HOUR FROM ata.processed_at AT TIME ZONE 'Asia/Kolkata')::int, COUNT(*)
        FROM activity_taskactivity ata
        JOIN account_details ad ON ata.account_id = ad.id
        WHERE ata.status = 'done'
          AND (ata.outcome <> 'RescheduledToNextDay' OR ata.outcome IS NULL)
          AND ad.account_allocation = 'System'
          AND DATE(ata.processed_at AT TIME ZONE 'Asia/Kolkata') = %s
          AND DATE(ata.eta AT TIME ZONE 'Asia/Kolkata') = %s
          AND EXTRACT(HOUR FROM ata.eta AT TIME ZONE 'Asia/Kolkata') =
              EXTRACT(HOUR FROM ata.processed_at AT TIME ZONE 'Asia/Kolkata')
          AND EXTRACT(HOUR FROM ata.processed_at AT TIME ZONE 'Asia/Kolkata') = ANY(%s)
        GROUP BY 1
    """, (today_ist, today_ist, HOUR_SLOTS))
    sched_exec = {r[0]: r[1] for r in cur.fetchall()}

    # Column 3: All executed in slot today (irrespective of eta)
    cur.execute("""
        SELECT EXTRACT(HOUR FROM ata.processed_at AT TIME ZONE 'Asia/Kolkata')::int, COUNT(*)
        FROM activity_taskactivity ata
        JOIN account_details ad ON ata.account_id = ad.id
        WHERE ata.status = 'done'
          AND (ata.outcome <> 'RescheduledToNextDay' OR ata.outcome IS NULL)
          AND ad.account_allocation = 'System'
          AND DATE(ata.processed_at AT TIME ZONE 'Asia/Kolkata') = %s
          AND EXTRACT(HOUR FROM ata.processed_at AT TIME ZONE 'Asia/Kolkata') = ANY(%s)
        GROUP BY 1
    """, (today_ist, HOUR_SLOTS))
    executed = {r[0]: r[1] for r in cur.fetchall()}

    return {'enqueued': enqueued, 'sched_exec': sched_exec, 'executed': executed}


# ── Section 3: AI Call Quality (Hourly) ──────────────────────────────────────

def fetch_section3(cur, today_ist):
    cur.execute("""
        SELECT
            EXTRACT(HOUR FROM ata.processed_at AT TIME ZONE 'Asia/Kolkata')::int,
            COUNT(*),
            COUNT(*) FILTER (WHERE ata.task_status = 'Connected'),
            COUNT(*) FILTER (WHERE ata.task_status = 'Connected' AND ata.call_duration > 20),
            COUNT(*) FILTER (WHERE ata.task_status = 'Connected' AND ata.call_duration > 120)
        FROM activity_taskactivity ata
        JOIN account_details ad ON ata.account_id = ad.id
        WHERE ata.status = 'done'
          AND (ata.outcome <> 'RescheduledToNextDay' OR ata.outcome IS NULL)
          AND ad.account_allocation = 'System'
          AND DATE(ata.processed_at AT TIME ZONE 'Asia/Kolkata') = %s
          AND EXTRACT(HOUR FROM ata.processed_at AT TIME ZONE 'Asia/Kolkata') = ANY(%s)
        GROUP BY 1
    """, (today_ist, HOUR_SLOTS))
    return {r[0]: (r[1], r[2], r[3], r[4]) for r in cur.fetchall()}


# ── Section 4: Disposition Breakdown (Hourly) ────────────────────────────────

def fetch_section4(cur, today_ist):
    # RescheduledToNextDay is included as its own disposition row
    cur.execute("""
        SELECT
            EXTRACT(HOUR FROM ata.processed_at AT TIME ZONE 'Asia/Kolkata')::int,
            ata.disposition,
            COUNT(*)
        FROM activity_taskactivity ata
        JOIN account_details ad ON ata.account_id = ad.id
        WHERE ata.status = 'done'
          AND ad.account_allocation = 'System'
          AND DATE(ata.processed_at AT TIME ZONE 'Asia/Kolkata') = %s
          AND EXTRACT(HOUR FROM ata.processed_at AT TIME ZONE 'Asia/Kolkata') = ANY(%s)
          AND ata.disposition IS NOT NULL AND ata.disposition <> ''
        GROUP BY 1, 2
    """, (today_ist, HOUR_SLOTS))
    data = {}
    for hour, disposition, count in cur.fetchall():
        data.setdefault(disposition, {})[int(hour)] = count
    return data


# ── Section 5: Flow Type Breakdown (Hourly) ──────────────────────────────────

def fetch_section5(cur, today_ist):
    cur.execute("""
        SELECT
            EXTRACT(HOUR FROM ata.processed_at AT TIME ZONE 'Asia/Kolkata')::int,
            ata.flow,
            COUNT(*)
        FROM activity_taskactivity ata
        JOIN account_details ad ON ata.account_id = ad.id
        WHERE ata.status = 'done'
          AND (ata.outcome <> 'RescheduledToNextDay' OR ata.outcome IS NULL)
          AND ad.account_allocation = 'System'
          AND DATE(ata.processed_at AT TIME ZONE 'Asia/Kolkata') = %s
          AND EXTRACT(HOUR FROM ata.processed_at AT TIME ZONE 'Asia/Kolkata') = ANY(%s)
          AND ata.flow IS NOT NULL AND ata.flow <> ''
        GROUP BY 1, 2
    """, (today_ist, HOUR_SLOTS))
    data = {}
    for hour, flow, count in cur.fetchall():
        data.setdefault(flow, {})[int(hour)] = count
    return data


# ── Formatters ────────────────────────────────────────────────────────────────

def format_section1(data, now_ist):
    ai  = 'System'
    man = 'Manual'
    lw  = 42
    cw  = 14

    header = f"{'Metric':<{lw}}{'AI (System)':>{cw}}{'Manual':>{cw}}"
    sep    = "-" * len(header)

    rows_data = [
        ("Total cases",
         data['total'].get(ai, 0),              data['total'].get(man, 0)),
        ("Calls done today",
         data['done'].get(ai, (0, 0))[0],       data['done'].get(man, (0, 0))[0]),
        ("Calls done today (unique cases)",
         data['done'].get(ai, (0, 0))[1],       data['done'].get(man, (0, 0))[1]),
        ("Calls enqueued for today",
         data['enqueued'].get(ai, (0, 0))[0],   data['enqueued'].get(man, (0, 0))[0]),
        ("Calls enqueued for today (unique cases)",
         data['enqueued'].get(ai, (0, 0))[1],   data['enqueued'].get(man, (0, 0))[1]),
        ("No activity done & enqueued today",
         data['no_activity'].get(ai, 0),         data['no_activity'].get(man, 0)),
    ]

    table_rows = [f"{lbl:<{lw}}{av:>{cw}}{mv:>{cw}}" for lbl, av, mv in rows_data]
    table = "\n".join([header, sep] + table_rows)

    return (
        f":bar_chart: *MFI Dashboard — Summary Stats* | "
        f"{now_ist.strftime('%d %b %Y, %I:%M %p')} IST\n"
        f"```\n{table}\n```"
    )


def format_section2(data, now_ist):
    hw = 10
    cw = 14

    header = (f"{'Hour':<{hw}}{'Enqueued':>{cw}}"
              f"{'Sched & Exec':>{cw}}{'Executed':>{cw}}")
    sep    = "-" * len(header)

    rows = []
    for h in HOUR_SLOTS:
        rows.append(
            f"{HOUR_LABELS[h]:<{hw}}"
            f"{data['enqueued'].get(h, 0):>{cw}}"
            f"{data['sched_exec'].get(h, 0):>{cw}}"
            f"{data['executed'].get(h, 0):>{cw}}"
        )

    table = "\n".join([header, sep] + rows)

    return (
        f":hourglass_flowing_sand: *MFI Dashboard — AI Call Queue vs Processed* | "
        f"{now_ist.strftime('%d %b %Y, %I:%M %p')} IST\n"
        f"_Enqueued = eta in slot | Sched & Exec = scheduled & executed in same slot | "
        f"Executed = all done in slot_\n"
        f"```\n{table}\n```"
    )


def format_section3(data, now_ist):
    hw = 10
    cw = 11

    header = (f"{'Hour':<{hw}}{'Done':>{cw}}{'Connected':>{cw}}"
              f"{'> 20s':>{cw}}{'> 120s':>{cw}}")
    sep    = "-" * len(header)

    rows = []
    for h in HOUR_SLOTS:
        done, conn, c20, c120 = data.get(h, (0, 0, 0, 0))
        rows.append(
            f"{HOUR_LABELS[h]:<{hw}}{done:>{cw}}{conn:>{cw}}{c20:>{cw}}{c120:>{cw}}"
        )

    table = "\n".join([header, sep] + rows)

    return (
        f":telephone_receiver: *MFI Dashboard — AI Call Quality (Hourly)* | "
        f"{now_ist.strftime('%d %b %Y, %I:%M %p')} IST\n"
        f"```\n{table}\n```"
    )


def format_section4(data, now_ist):
    lw = 32
    cw = 6

    short = [HOUR_SHORT[h] for h in HOUR_SLOTS]
    header = f"{'Disposition':<{lw}}" + "".join(f"{l:>{cw}}" for l in short)
    sep    = "-" * len(header)

    rows = []
    for disp in DISPOSITIONS:
        hour_data = data.get(disp, {})
        if not any(hour_data.get(h, 0) for h in HOUR_SLOTS):
            continue
        row = f"{disp:<{lw}}" + "".join(f"{hour_data.get(h, 0):>{cw}}" for h in HOUR_SLOTS)
        rows.append(row)

    if not rows:
        rows = ["No data for today"]

    table = "\n".join([header, sep] + rows)

    return (
        f":memo: *MFI Dashboard — Disposition Breakdown (Hourly)* | "
        f"{now_ist.strftime('%d %b %Y, %I:%M %p')} IST\n"
        f"```\n{table}\n```"
    )


def format_section5(data, now_ist):
    lw = 24
    cw = 6

    short = [HOUR_SHORT[h] for h in HOUR_SLOTS]
    header = f"{'Flow Type':<{lw}}" + "".join(f"{l:>{cw}}" for l in short)
    sep    = "-" * len(header)

    rows = []
    for flow in FLOWS:
        hour_data = data.get(flow, {})
        row = f"{flow:<{lw}}" + "".join(f"{hour_data.get(h, 0):>{cw}}" for h in HOUR_SLOTS)
        rows.append(row)

    table = "\n".join([header, sep] + rows)

    return (
        f":arrows_counterclockwise: *MFI Dashboard — Flow Type Breakdown (Hourly)* | "
        f"{now_ist.strftime('%d %b %Y, %I:%M %p')} IST\n"
        f"```\n{table}\n```"
    )


# ── Slack ─────────────────────────────────────────────────────────────────────

def send_to_slack(message):
    payload = json.dumps({"text": message}).encode()
    req = urllib.request.Request(
        WEBHOOK_URL, data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    resp = urllib.request.urlopen(req)
    print(f"Sent to Slack: {resp.status}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not is_within_business_hours():
        print("Outside business hours (9AM–8PM IST). Skipping.")
        return

    now_ist   = datetime.now(IST)
    today_ist = now_ist.date()
    print(f"Fetching MFI dashboard data for {today_ist}...")

    conn = get_connection()
    cur  = conn.cursor()

    s1 = fetch_section1(cur, today_ist)
    s2 = fetch_section2(cur, today_ist)
    s3 = fetch_section3(cur, today_ist)
    s4 = fetch_section4(cur, today_ist)
    s5 = fetch_section5(cur, today_ist)

    cur.close()
    conn.close()

    for msg in [
        format_section1(s1, now_ist),
        format_section2(s2, now_ist),
        format_section3(s3, now_ist),
        format_section4(s4, now_ist),
        format_section5(s5, now_ist),
    ]:
        send_to_slack(msg)

    print("Done.")


if __name__ == "__main__":
    main()
