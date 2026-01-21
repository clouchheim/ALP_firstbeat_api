import os
import requests
import pandas as pd
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

# =========================
# ENV / CONSTANTS
# =========================
load_dotenv()
SB_BASE_URL = os.getenv("SB_BASE_URL")
SB_USERNAME = os.getenv("SB_USERNAME")
SB_PASSWORD = os.getenv("SB_PASSWORD")
SB_APP_ID   = os.getenv("SB_APP_ID", "firstbeat-sync")

if not SB_USERNAME or not SB_PASSWORD:
    raise RuntimeError("Missing SB_USERNAME or SB_PASSWORD in environment")

# =========================
# AUTH / HEADERS
# =========================

def _sb_headers():
    return {
        "X-APP-ID": SB_APP_ID,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

def _sb_auth():
    return HTTPBasicAuth(SB_USERNAME, SB_PASSWORD)

# =========================
# USERS (CORRECT ENDPOINT)
# =========================

def get_usss_user_map():
    """
    Uses /usersynchronise to retrieve all accessible users.

    Returns:
        dict[(first_name, last_name)] -> user_id
    """
    url = f"{SB_BASE_URL}/api/v1/usersynchronise?informat=json&format=json"

    payload = {
        "lastSynchronisationTimeOnServer": 0,
        "paginate": True
    }

    user_map = {}

    while True:
        r = requests.post(
            url,
            headers=_sb_headers(),
            auth=_sb_auth(),
            json=payload,
            timeout=60
        )
        r.raise_for_status()
        data = r.json()

        users = data.get("users", [])
        for u in users:
            key = (u["firstName"].strip(), u["lastName"].strip())
            user_map[key] = u["userId"]

        cursor = data.get("nextCursor")
        if not cursor:
            break

        payload["cursor"] = cursor

    return user_map

# =========================
# EXISTING EVENTS (DEDUP)
# =========================

def get_existing_measurement_ids(user_ids, form_name):
    """
    Pulls existing form events
    and returns a set of measurement IDs already uploaded.
    """
    if not user_ids:
        return set()

    url = f"{SB_BASE_URL}/api/v1/synchronise?informat=json&format=json"

    payload = {
        "formName": form_name,
        "lastSynchronisationTimeOnServer": 0,
        "userIds": list(set(user_ids)),
        "paginate": True
    }

    existing_ids = set()

    while True:
        r = requests.post(
            url,
            headers=_sb_headers(),
            auth=_sb_auth(),
            json=payload,
            timeout=60
        )
        r.raise_for_status()
        data = r.json()

        events = data.get("export", {}).get("events", [])
        for event in events:
            for row in event.get("rows", []):
                for pair in row.get("pairs", []):
                    if pair.get("key") == "ID":
                        existing_ids.add(pair.get("value"))

        cursor = data.get("export", {}).get("nextCursor")
        if not cursor:
            break

        payload["cursor"] = cursor

    return existing_ids

# =========================
# EVENT PAYLOAD
# =========================

def _build_event_payload(row, form_name):
    return {
        "formName": form_name,
        "startDate": row["start_date"],
        "startTime": row["start_time"],
        "finishDate": row["end_date"],
        "finishTime": row["end_time"],
        "userId": {"userId": int(row["user_id"])},
        "rows": [
            {
                "row": 0,
                "pairs": [
                    {"key": "ID", "value": row["ID"]},
                    {"key": "Session Type", "value": row["Session Type"]},
                    {"key": "ACWR", "value": str(row["ACWR"])}, # yes i know that these look flipped
                    {"key": "RMSSD", "value": str(row["RMSSD"])}
                ]
            }
        ]
    }

# =========================
# PUBLIC ENTRY POINT
# =========================

def upload_dataframe(df: pd.DataFrame, form_name, verbose: bool = True) -> int:
    """
    Uploads form name data into Smartabase.
    NOTE: DataFrame must contain the correct that match the form in 
    """

    # check that required rows are in here
    required_columns = ["First Name", "Last Name", "ID"]
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"DataFrame must contain the following columns: {required_columns}")

    # check if there is data to upload
    if df.empty:
        if verbose:
            print("No data rows provided. Nothing to upload.")
        return 0

    # -------------------------
    # Map users
    # -------------------------
    user_map = get_usss_user_map()

    df = df.copy()
    df["user_id"] = df.apply(
        lambda r: user_map.get((r["First Name"], r["Last Name"])),
        axis=1
    )

    df = df[df["user_id"].notna()]

    if df.empty:
        if verbose:
            print("No rows matched Smartabase users. Nothing to upload.")
        return 0

    # -------------------------
    # Remove duplicates
    # -------------------------
    existing_ids = get_existing_measurement_ids(df["user_id"].tolist(), form_name)
    df = df[~df["ID"].isin(existing_ids)]

    if df.empty:
        if verbose:
            print("All rows already exist in Smartabase. Nothing to upload.")
        return 0

    # -------------------------
    # Upload
    # -------------------------
    url = f"{SB_BASE_URL}/api/v1/eventimport?informat=json&format=json"

    success_count = 0

    for _, row in df.iterrows():
        payload = _build_event_payload(row)

        r = requests.post(
            url,
            headers=_sb_headers(),
            auth=_sb_auth(),
            json=payload,
            timeout=30
        )

        if r.status_code == 200:
            success_count += 1
            if verbose:
                print(f"Uploaded: {row['ID']}")
        else:
            print(f"FAILED ({row['ID']}): {r.status_code} - {r.text}")

    if verbose:
        print(f"Successfully uploaded {success_count} {form_name} events.")

    return success_count
