import sqlite3
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import requests
import time

load_dotenv()

# Load DB path from environment or default to local file
db_path = os.getenv("P1_LIST_PATH")
if not db_path:
    raise ValueError("P1_LIST_PATH is not set in the environment")


# Connect to the database (creates it if it doesn't exist)
conn = sqlite3.connect(db_path)
cursor = conn.cursor()


# Update loop
def update_db():
    print("========= Syncing Database =========")
    data_sync()
    print("\nUpdating hours for new user(s).")
    update_null_hours()
    print("Hour update complete.\n\nUpdating 3-month check start dates.")
    update_null_check_start_dates()
    print("Start date update complete.")


# Sync DB with latest update.csv file
def data_sync():
    new_data = pd.read_csv('Data\\update.csv')
    current_data = pd.read_sql_query('SELECT cid FROM LIST', conn)

    # Normalize cid columns to ensure accurate comparison
    new_data['cid'] = new_data['cid'].astype(str).str.strip()
    current_data['cid'] = current_data['cid'].astype(str).str.strip()

    # 1. Find rows in new_data that are not in current_data (to add)
    to_add = new_data[~new_data['cid'].isin(current_data['cid'])]

    # 2. Find rows in current_data that are not in new_data (to remove)
    to_remove = current_data[~current_data['cid'].isin(new_data['cid'])]

    # 3. Print and execute deletions
    print("The following CIDs will be deleted from the database:")
    for cid in to_remove['cid']:
        print(f" - Deleting cid: {cid}")
        cursor.execute("DELETE FROM LIST WHERE cid = ?", (cid,))

    # 4. Print and execute additions
    print("The following new users will be added to the database:")
    for _, row in to_add.iterrows():
        print(f" - Adding cid: {row['cid']}, list_join_date: {row['join_date']}")
        cursor.execute("""
            INSERT INTO LIST (cid, list_join_date, pilot_hours, atc_hours, three_month_check_start_date)
            VALUES (?, ?, NULL, NULL, NULL)
        """, (row['cid'], row['join_date']))

    # Review
    print(f"\n{len(to_remove)} user(s) scheduled for deletion.")
    print(f"{len(to_add)} user(s) scheduled for addition.")

    # Commit
    conn.commit()
    print("Changes committed.")


# Make sure there are no NULL hour values
def update_null_hours():
    null_hour_cid_df = pd.read_sql_query('SELECT cid FROM LIST WHERE pilot_hours IS NULL OR atc_hours IS NULL', conn)
    update_hours(null_hour_cid_df)


# Updates a list of hours for a list of CIDs. Input a dataframe with a column named "cid". 7 sec delay ensures you remain below 10 requests/min for VATSIM API.
def update_hours(cid_df):
    for index, row in cid_df.iterrows():
        cid = row['cid']
        pilot_hours, atc_hours = get_hours(cid, "1995-01-01T00:00:00+00:00", "2100-01-01T00:00:00+00:00")
        cursor.execute("""
            UPDATE LIST
            SET pilot_hours = ?, atc_hours = ?
            WHERE cid = ?
        """, (pilot_hours, atc_hours, cid))
        conn.commit()
        time.sleep(7)


# VATSIM API call to get hours
def get_hours(cid, start, end):
    pilot_hours = get_pilot_hours(cid, start, end)
    time.sleep(7)
    atc_hours = get_atc_hours(cid, start, end)
    return round(pilot_hours, 2), round(atc_hours, 2)
    

def get_pilot_hours(cid, start, end):
    start_range = datetime.fromisoformat(start)
    end_range = datetime.fromisoformat(end)

    url = f"https://api.vatsim.net/v2/members/{cid}/history?limit=10000"
    headers = {'Accept': 'application/json'}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        return 0

    data = response.json()
    total_seconds = 0

    for session in data.get('items', []):
        start_str = session.get('start')
        end_str = session.get('end')

        if not start_str or not end_str:
            continue

        start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
        end = datetime.fromisoformat(end_str.replace("Z", "+00:00"))

        if end < start_range or start > end_range:
            continue

        actual_start = max(start, start_range)
        actual_end = min(end, end_range)
        total_seconds += (actual_end - actual_start).total_seconds()

    return total_seconds / 3600  # Return in hours


def get_atc_hours(cid, start, end):
    start_range = datetime.fromisoformat(start)
    end_range = datetime.fromisoformat(end)

    url = f"https://api.vatsim.net/v2/members/{cid}/atc?limit=10000"
    headers = {'Accept': 'application/json'}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        return 0

    data = response.json()
    total_seconds = 0

    for session in data.get('items', []):
        conn = session.get('connection_id', {})
        start_str = conn.get('start')
        end_str = conn.get('end')

        if not start_str or not end_str:
            continue

        start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))

        if end_dt < start_range or start_dt > end_range:
            continue

        actual_start = max(start_dt, start_range)
        actual_end = min(end_dt, end_range)
        total_seconds += (actual_end - actual_start).total_seconds()

    return total_seconds / 3600  # Return in hours
    

# Change the null 3 month checker start dates to the first of the next month
def update_null_check_start_dates():
    cursor.execute("""
        SELECT cid, list_join_date
        FROM LIST
        WHERE three_month_check_start_date IS NULL
    """)

    rows = cursor.fetchall()

    for cid, join_date_str in rows:
        join_date = datetime.strptime(join_date_str, '%d/%m/%Y %H:%M:%S')  # Adjust format if needed
        first_of_next_month = (join_date.replace(day=1) + timedelta(days=32)).replace(day=1)
        date_str = first_of_next_month.strftime('%Y-%m-%d')

        cursor.execute("""
            UPDATE LIST
            SET three_month_check_start_date = ?
            WHERE cid = ?
        """, (date_str, cid))

    conn.commit()


# Validate minimum hours
def minimum_hours_checker():
    # Current rules:
    # 1) 30 hours minimum for the waiting list
    # 2) 15 hours minimum for anyone with ATC time

    # SQL query to find violating users
    query = """
    SELECT cid
    FROM LIST
    WHERE
        (
            atc_hours = 0 AND pilot_hours < 30
        )
        OR
        (
            atc_hours > 0 AND pilot_hours < 15
        );
    """

    # Load result into DataFrame
    violators_df = pd.read_sql_query(query, conn)

    # Get the list of violating CIDs
    violating_cids = violators_df['cid'].tolist()

    print("\n\n========= Minimum hour checks =========")
    print("CIDs which do NOT meet the minimum hour requirements:")
    print('\n'.join(violating_cids))


def activity_checker():
    # Current rules:
    # 1) 10 hours every 3 months
    # 2) 5 hours every 3 months if on ATC roster (NOT IMPLEMENTED)

    today = datetime.today()
    target_year = today.year
    target_month = today.month - 3
    if target_month <= 0:
        target_month += 12
        target_year -= 1

    target_date = datetime(target_year, target_month, 1)
    target_str = target_date.strftime('%Y-%m-%d')
    target_str_full = target_date.strftime('%Y-%m-%dT00:00:00+00:00')

    current_month_start = datetime(today.year, today.month, 1)
    prev_day = current_month_start - timedelta(seconds=1)
    prev_day_str_full = prev_day.strftime('%Y-%m-%dT%H:%M:%S+00:00')

    query = """
    SELECT cid, pilot_hours
    FROM LIST
    WHERE three_month_check_start_date = ?
    """

    df = pd.read_sql_query(query, conn, params=(target_str,))[['cid', 'pilot_hours']]
    df['active'] = False

    print("\n\n========= Activity checks =========")
    for index, row in df.iterrows():
        cid = row.iloc[0]
        pilot_hours, atc_hours = get_hours(cid,target_str_full, prev_day_str_full)

        # Meets requirements
        if float(pilot_hours) - float(row.iloc[1]) >= 10:
            df.loc[df['cid'] == cid, 'active'] = True
            first_of_month = datetime.today().replace(day=1).strftime('%Y-%m-%d')
            cursor.execute("""
                UPDATE LIST
                SET pilot_hours = ?, atc_hours = ?, three_month_check_start_date = ?
                WHERE cid = ?
            """, (pilot_hours, atc_hours, first_of_month, cid))
            conn.commit()

        time.sleep(7) # prevent timeout

    print(f"Hours for {len(df[df['active'] == True])} active user(s) updated:")
    print('\n'.join(df[df['active'] == True]['cid'].tolist()))
    print(f"\n{len(df[df['active'] == False])} inactive user(s) found:")
    print('\n'.join(df[df['active'] == False]['cid'].tolist()))

        


update_db()
minimum_hours_checker()
activity_checker()


# Close connection
conn.close()
