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

# Updated: removed stale entries, added new dispositions from DB, removed RescheduledToNextDay
# (always zero after outcome filter was fixed)
DISPOSITIONS = [
    "Agree To Pay",
    "Agree To Senior Manager Call",
    "Busy",
    "Call Back Requested",
    "Call Hang Up",
    "Call Hung Up",
    "Disconnected",
    "Dispute",
    "Failed",
    "Financial Hardship",
    "Follow Up",
    "Incoming Call Barred",
    "Information Conveyed",
    "Invalid Number",
    "No Answer",
    "Not Reachable",
    "Paid On Call",
    "Partial Payment",
    "Payment Claimed",
    "Payment Paid",
    "Promise To Pay",
    "Refuse To Pay",
    "Requested Settlement",
    "Settlement Not Concluded",
    "Switched Off",
    "Third Party Connect",
    "Unacceptable Promise To Pay",
    "Unclear",
    "Unpaid",
    "Will Pay",
    "Wrong Number",
]

# Fixed: flow names now match actual DB values
FLOWS = ["fusion_mfi_emi", "fusion_mfi_explore", "fusion_mfi_rnr", "fusion_mfi_settlement"]
FLOW_LABELS = {
    "fusion_mfi_emi":        "EMI Collection",
    "fusion_mfi_explore":    "Explore / Long Overdue",
    "fusion_mfi_rnr":        "RNR",
    "fusion_mfi_settlement": "Settlement",
}

# High-intent dispositions shown in sub-disposition table
SUBDISPOSITION_FOCUS = [
    "Agree To Pay",
    "Promise To Pay",
    "Settlement Not Concluded",
    "Dispute",
    "Financial Hardship",
    "Third Party Connect",
    "Refuse To Pay",
]


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
    cur.execute("""
        SELECT account_allocation, COUNT(*)
        FROM account_details
        WHERE account_allocation IN ('System', 'Manual')
        GROUP BY account_allocation
    """)
    total = {r[0]: r[1] for r in cur.fetchall()}

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

    cur.execute("""
        SELECT ad.account_allocation, COUNT(*), COUNT(DISTINCT ata.account_id)
        FROM activity_taskactivity ata
        JOIN account_details ad ON ata.account_id = ad.id
        WHERE ata.status = 'enqueued'
          AND DATE(ata.eta AT TIME ZONE 'Asia/Kolkata') = %s
        GROUP BY ad.account_allocation
    """, (today_ist,))
    enqueued = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

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


# ── Section 3: AI Call Quality + Sentiment (Hourly) ──────────────────────────

def fetch_section3(cur, today_ist):
    cur.execute("""
        SELECT
            EXTRACT(HOUR FROM ata.processed_at AT TIME ZONE 'Asia/Kolkata')::int,
            COUNT(*),
            COUNT(*) FILTER (WHERE ata.task_status = 'Connected'),
            COUNT(*) FILTER (WHERE ata.task_status = 'Connected' AND ata.call_duration > 20),
            COUNT(*) FILTER (WHERE ata.task_status = 'Connected' AND ata.call_duration > 120),
            COUNT(*) FILTER (WHERE ata.sentiment = 'positive'),
            COUNT(*) FILTER (WHERE ata.sentiment = 'neutral'),
            COUNT(*) FILTER (WHERE ata.sentiment = 'negative')
        FROM activity_taskactivity ata
        JOIN account_details ad ON ata.account_id = ad.id
        WHERE ata.status = 'done'
          AND (ata.outcome <> 'RescheduledToNextDay' OR ata.outcome IS NULL)
          AND ad.account_allocation = 'System'
          AND DATE(ata.processed_at AT TIME ZONE 'Asia/Kolkata') = %s
          AND EXTRACT(HOUR FROM ata.processed_at AT TIME ZONE 'Asia/Kolkata') = ANY(%s)
        GROUP BY 1
    """, (today_ist, HOUR_SLOTS))
    return {r[0]: (r[1], r[2], r[3], r[4], r[5], r[6], r[7]) for r in cur.fetchall()}


# ── Section 4: Disposition Breakdown (Hourly) ────────────────────────────────

def fetch_section4(cur, today_ist):
    # Fixed: added outcome filter so numbers are consistent with other tables
    cur.execute("""
        SELECT
            EXTRACT(HOUR FROM ata.processed_at AT TIME ZONE 'Asia/Kolkata')::int,
            ata.disposition,
            COUNT(*)
        FROM activity_taskactivity ata
        JOIN account_details ad ON ata.account_id = ad.id
        WHERE ata.status = 'done'
          AND (ata.outcome <> 'RescheduledToNextDay' OR ata.outcome IS NULL)
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


# ── Section 6: Calls by DPD Bucket ───────────────────────────────────────────

def fetch_section6(cur, today_ist):
    cur.execute("""
        SELECT
            ad.dpd_bucket,
            COUNT(*) AS calls,
            COUNT(DISTINCT ata.account_id) AS accounts,
            COUNT(*) FILTER (WHERE ata.task_status = 'Connected') AS connected,
            COUNT(*) FILTER (WHERE ata.disposition IN (
                'Agree To Pay', 'Promise To Pay', 'Payment Claimed',
                'Paid On Call', 'Payment Paid'
            )) AS positive
        FROM activity_taskactivity ata
        JOIN account_details ad ON ata.account_id = ad.id
        WHERE ata.status = 'done'
          AND (ata.outcome <> 'RescheduledToNextDay' OR ata.outcome IS NULL)
          AND ad.account_allocation = 'System'
          AND DATE(ata.processed_at AT TIME ZONE 'Asia/Kolkata') = %s
        GROUP BY ad.dpd_bucket
        ORDER BY ad.dpd_bucket
    """, (today_ist,))
    return cur.fetchall()


# ── Section 7: Sub-disposition Breakdown (High-intent dispositions) ───────────

def fetch_section7(cur, today_ist):
    cur.execute("""
        SELECT disposition, sub_disposition, COUNT(*)
        FROM activity_taskactivity ata
        JOIN account_details ad ON ata.account_id = ad.id
        WHERE ata.status = 'done'
          AND (ata.outcome <> 'RescheduledToNextDay' OR ata.outcome IS NULL)
          AND ad.account_allocation = 'System'
          AND DATE(ata.processed_at AT TIME ZONE 'Asia/Kolkata') = %s
          AND ata.disposition = ANY(%s)
          AND ata.sub_disposition IS NOT NULL AND ata.sub_disposition <> ''
        GROUP BY disposition, sub_disposition
        ORDER BY disposition, COUNT(*) DESC
    """, (today_ist, SUBDISPOSITION_FOCUS))
    data = {}
    for disp, sub, cnt in cur.fetchall():
        data.setdefault(disp, []).append((sub, cnt))
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
    cw = 13

    header = (f"{'Hour':<{hw}}{'Enqueued':>{cw}}"
              f"{'Sched+Exec':>{cw}}{'Executed':>{cw}}{'Spillover':>{cw}}")
    sep = "-" * len(header)

    rows = []
    t_enq = t_sched = t_exec = t_spill = 0
    for h in HOUR_SLOTS:
        enq_v   = data['enqueued'].get(h, 0)
        sched_v = data['sched_exec'].get(h, 0)
        exec_v  = data['executed'].get(h, 0)
        spill_v = exec_v - sched_v
        t_enq   += enq_v
        t_sched += sched_v
        t_exec  += exec_v
        t_spill += spill_v
        rows.append(
            f"{HOUR_LABELS[h]:<{hw}}{enq_v:>{cw}}{sched_v:>{cw}}{exec_v:>{cw}}{spill_v:>{cw}}"
        )

    total_row = f"{'TOTAL':<{hw}}{t_enq:>{cw}}{t_sched:>{cw}}{t_exec:>{cw}}{t_spill:>{cw}}"
    table = "\n".join([header, sep] + rows + [sep, total_row])

    return (
        f":hourglass_flowing_sand: *MFI Dashboard — AI Call Queue vs Processed* | "
        f"{now_ist.strftime('%d %b %Y, %I:%M %p')} IST\n"
        f"_Enqueued = eta in slot | Sched+Exec = scheduled & executed in same slot | "
        f"Executed = all done in slot | Spillover = Executed - Sched+Exec_\n"
        f"```\n{table}\n```"
    )


def format_section3(data, now_ist):
    hw = 10
    cw = 10

    header = (f"{'Hour':<{hw}}{'Done':>{cw}}{'Connected':>{cw}}"
              f"{'>20s':>{cw}}{'>120s':>{cw}}{'Pos':>{cw}}{'Neutral':>{cw}}{'Neg':>{cw}}")
    sep = "-" * len(header)

    rows = []
    t_done = t_conn = t_c20 = t_c120 = t_pos = t_neu = t_neg = 0
    for h in HOUR_SLOTS:
        done, conn, c20, c120, pos, neu, neg = data.get(h, (0, 0, 0, 0, 0, 0, 0))
        t_done += done; t_conn += conn; t_c20 += c20; t_c120 += c120
        t_pos  += pos;  t_neu  += neu;  t_neg  += neg
        rows.append(
            f"{HOUR_LABELS[h]:<{hw}}{done:>{cw}}{conn:>{cw}}"
            f"{c20:>{cw}}{c120:>{cw}}{pos:>{cw}}{neu:>{cw}}{neg:>{cw}}"
        )

    total_row = (
        f"{'TOTAL':<{hw}}{t_done:>{cw}}{t_conn:>{cw}}"
        f"{t_c20:>{cw}}{t_c120:>{cw}}{t_pos:>{cw}}{t_neu:>{cw}}{t_neg:>{cw}}"
    )
    table = "\n".join([header, sep] + rows + [sep, total_row])

    return (
        f":telephone_receiver: *MFI Dashboard — AI Call Quality & Sentiment* | "
        f"{now_ist.strftime('%d %b %Y, %I:%M %p')} IST\n"
        f"_Sentiment (Pos/Neutral/Neg) is recorded only for connected calls_\n"
        f"```\n{table}\n```"
    )


def format_section4(data, now_ist):
    lw = 30
    cw = 6

    short  = [HOUR_SHORT[h] for h in HOUR_SLOTS]
    header = f"{'Disposition':<{lw}}" + "".join(f"{l:>{cw}}" for l in short) + f"{'TOTAL':>{cw+2}}"
    sep    = "-" * len(header)

    rows = []
    for disp in DISPOSITIONS:
        hour_data  = data.get(disp, {})
        row_total  = sum(hour_data.get(h, 0) for h in HOUR_SLOTS)
        row = (f"{disp:<{lw}}"
               + "".join(f"{hour_data.get(h, 0):>{cw}}" for h in HOUR_SLOTS)
               + f"{row_total:>{cw+2}}")
        rows.append(row)

    table = "\n".join([header, sep] + rows)

    return (
        f":memo: *MFI Dashboard — Disposition Breakdown (Hourly)* | "
        f"{now_ist.strftime('%d %b %Y, %I:%M %p')} IST\n"
        f"_Filter: status=done | outcome ≠ RescheduledToNextDay | AI calls only_\n"
        f"```\n{table}\n```"
    )


def format_section5(data, now_ist):
    lw = 26
    cw = 6

    short  = [HOUR_SHORT[h] for h in HOUR_SLOTS]
    header = f"{'Flow':<{lw}}" + "".join(f"{l:>{cw}}" for l in short) + f"{'TOTAL':>{cw+2}}"
    sep    = "-" * len(header)

    rows = []
    for flow in FLOWS:
        hour_data = data.get(flow, {})
        row_total = sum(hour_data.get(h, 0) for h in HOUR_SLOTS)
        label = FLOW_LABELS[flow]
        row = (f"{label:<{lw}}"
               + "".join(f"{hour_data.get(h, 0):>{cw}}" for h in HOUR_SLOTS)
               + f"{row_total:>{cw+2}}")
        rows.append(row)

    table = "\n".join([header, sep] + rows)

    return (
        f":arrows_counterclockwise: *MFI Dashboard — Flow Type Breakdown (Hourly)* | "
        f"{now_ist.strftime('%d %b %Y, %I:%M %p')} IST\n"
        f"```\n{table}\n```"
    )


def format_section6(rows, now_ist):
    lw = 32
    cw = 12

    header = (f"{'DPD Bucket':<{lw}}{'Calls':>{cw}}"
              f"{'Accounts':>{cw}}{'Connected':>{cw}}{'Positive':>{cw}}")
    sep = "-" * len(header)

    table_rows = []
    t_calls = t_accs = t_conn = t_pos = 0
    for bucket, calls, accs, conn_, pos in rows:
        b = str(bucket) if bucket else "(null)"
        table_rows.append(
            f"{b:<{lw}}{calls:>{cw}}{accs:>{cw}}{conn_:>{cw}}{pos:>{cw}}"
        )
        t_calls += calls; t_accs += accs; t_conn += conn_; t_pos += pos

    total_row = f"{'TOTAL':<{lw}}{t_calls:>{cw}}{t_accs:>{cw}}{t_conn:>{cw}}{t_pos:>{cw}}"
    table = "\n".join([header, sep] + table_rows + [sep, total_row])

    return (
        f":bar_chart: *MFI Dashboard — Calls by DPD Bucket* | "
        f"{now_ist.strftime('%d %b %Y, %I:%M %p')} IST\n"
        f"_Positive = Agree To Pay / Promise To Pay / Payment Claimed / Paid On Call / Payment Paid_\n"
        f"```\n{table}\n```"
    )


def format_section7(data, now_ist):
    lw1 = 30
    lw2 = 36
    cw  = 8

    header = f"{'Disposition':<{lw1}}{'Sub-disposition':<{lw2}}{'Count':>{cw}}"
    sep    = "-" * len(header)

    rows = []
    for disp in SUBDISPOSITION_FOCUS:
        subs = data.get(disp)
        if not subs:
            continue
        for sub, cnt in subs:
            rows.append(f"{disp:<{lw1}}{sub:<{lw2}}{cnt:>{cw}}")

    if not rows:
        return None

    table = "\n".join([header, sep] + rows)

    return (
        f":mag: *MFI Dashboard — Sub-disposition Breakdown (High-intent)* | "
        f"{now_ist.strftime('%d %b %Y, %I:%M %p')} IST\n"
        f"_Dispositions: Agree To Pay | Promise To Pay | Settlement Not Concluded | "
        f"Dispute | Financial Hardship | Third Party Connect | Refuse To Pay_\n"
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
    s6 = fetch_section6(cur, today_ist)
    s7 = fetch_section7(cur, today_ist)

    cur.close()
    conn.close()

    messages = [
        format_section1(s1, now_ist),
        format_section2(s2, now_ist),
        format_section3(s3, now_ist),
        format_section4(s4, now_ist),
        format_section5(s5, now_ist),
        format_section6(s6, now_ist),
        format_section7(s7, now_ist),
    ]

    for msg in messages:
        if msg:
            send_to_slack(msg)

    print("Done.")


if __name__ == "__main__":
    main()
