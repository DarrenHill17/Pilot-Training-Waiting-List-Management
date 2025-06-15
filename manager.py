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
    update_null_hours()
    update_null_check_start_dates()


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
        pilot_hours, atc_hours = get_hours(cid)
        cursor.execute("""
            UPDATE LIST
            SET pilot_hours = ?, atc_hours = ?
            WHERE cid = ?
        """, (pilot_hours, atc_hours, cid))
        conn.commit()
        time.sleep(7)


# VATSIM API call to get hours
def get_hours(cid):
    url = f"https://api.vatsim.net/v2/members/{cid}/stats"
    headers = {'Accept': 'application/json'}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        pilot_hours = data.get("pilot", 0.0)
        keys_to_sum = ["s1", "s2", "s3", "c1", "c2", "c3", "i1", "i2", "i3", "sup", "adm"]
        other_hours = sum(data.get(key, 0.0) for key in keys_to_sum)
        return pilot_hours, other_hours
    else:
        return 0.0, 0.0
    

# Change the null 3 month checker start dates to the first of the next month
def update_null_check_start_dates():
    today = datetime.today()
    first_of_next_month = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
    date_str = first_of_next_month.strftime('%Y-%m-%d')

    cursor.execute("""
        UPDATE LIST
        SET three_month_check_start_date = ?
        WHERE three_month_check_start_date IS NULL
    """, (date_str,))

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
    year = today.year
    month = today.month - 3
    if month <= 0:
        month += 12
        year -= 1

    target_date = datetime(year, month, 1)
    target_str = target_date.strftime('%Y-%m-%d')

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
        pilot_hours, atc_hours = get_hours(cid)

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
