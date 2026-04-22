import os
import psycopg2
from slack_sdk import WebClient
from datetime import datetime, timedelta
import pytz

IST = pytz.timezone("Asia/Kolkata")

DB_HOST = "otolmsstagedbinstance.cttxlpcdrmsq.ap-south-1.rds.amazonaws.com"
DB_PORT = 5432
DB_NAME = "fusion_finance_mfi"
DB_USER = "readonly"
DB_PASS = "readonly"

SLACK_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL = os.environ["SLACK_CHANNEL_ID"]

HOURS = [9, 10, 11, 12, 13, 14, 15, 16, 17, 18]
HOUR_LABELS = [
    "9AM–10AM", "10AM–11AM", "11AM–12PM", "12PM–1PM",
    "1PM–2PM", "2PM–3PM", "3PM–4PM", "4PM–5PM", "5PM–6PM", "6PM–7PM"
]


def is_within_business_hours():
    now_ist = datetime.now(IST)
    return 9 <= now_ist.hour < 19


def get_last_7_days():
    today = datetime.now(IST).date()
    return [(today - timedelta(days=i)) for i in range(6, -1, -1)]


def fetch_data(dates):
    start = f"{dates[0]} 09:00:00"
    end = f"{dates[-1]} 19:00:00"

    query = """
        SELECT
            DATE(processed_at AT TIME ZONE 'Asia/Kolkata') AS date,
            EXTRACT(HOUR FROM processed_at AT TIME ZONE 'Asia/Kolkata') AS hour_ist,
            COUNT(*) AS call_count
        FROM activity_taskactivity
        WHERE processed_at IS NOT NULL
          AND status = 'done'
          AND (outcome != 'RescheduledToNextDay' OR outcome IS NULL)
          AND (processed_at AT TIME ZONE 'Asia/Kolkata') >= %s
          AND (processed_at AT TIME ZONE 'Asia/Kolkata') < %s
          AND EXTRACT(HOUR FROM processed_at AT TIME ZONE 'Asia/Kolkata') >= 9
          AND EXTRACT(HOUR FROM processed_at AT TIME ZONE 'Asia/Kolkata') < 19
        GROUP BY 1, 2
        ORDER BY 1, 2;
    """

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )
    cur = conn.cursor()
    cur.execute(query, (start, end))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    data = {}
    for date, hour, count in rows:
        data[(str(date), int(hour))] = count
    return data


def format_slack_message(dates, data):
    now_ist = datetime.now(IST)
    today = now_ist.date()

    col_width = 10
    date_labels = [d.strftime("%d %b") for d in dates]

    header = f"{'Hour':<12}" + "".join(f"{d:>{col_width}}" for d in date_labels)
    separator = "-" * len(header)

    rows = []
    totals = {str(d): 0 for d in dates}

    for hour, label in zip(HOURS, HOUR_LABELS):
        row = f"{label:<12}"
        for d in dates:
            count = data.get((str(d), hour), 0)
            totals[str(d)] += count
            is_live = (d == today and now_ist.hour == hour)
            cell = f"{count}*" if is_live else str(count)
            row += f"{cell:>{col_width}}"
        rows.append(row)

    total_row = f"{'TOTAL':<12}" + "".join(f"{totals[str(d)]:>{col_width}}" for d in dates)

    table = "\n".join([header, separator] + rows + [separator, total_row])

    message = (
        f":bar_chart: *MFI AI Call Execution Table* — "
        f"Updated at {now_ist.strftime('%d %b %Y, %I:%M %p')} IST\n"
        f"```\n{table}\n```\n"
        f"_Filters: processed\\_at 9AM–7PM IST | status=done | outcome≠RescheduledToNextDay_\n"
        f"_* = current live hour_"
    )
    return message


def send_to_slack(message):
    client = WebClient(token=SLACK_TOKEN)
    client.chat_postMessage(channel=SLACK_CHANNEL, text=message)
    print("Message sent to Slack.")


def main():
    if not is_within_business_hours():
        print("Outside business hours (9AM–7PM IST). Skipping.")
        return

    dates = get_last_7_days()
    print(f"Fetching data for {dates[0]} to {dates[-1]}...")
    data = fetch_data(dates)
    message = format_slack_message(dates, data)
    send_to_slack(message)


if __name__ == "__main__":
    main()
