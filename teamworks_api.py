import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
import time

'''
Helper functions to upload dataframes into Smartabase via the API.
This module assumes that the dataframe being uploadled has been cleaned, and has the correct + required columns/formatting 
to match the target form in Smartabase.
It also assumes that the username and password supplied to the CORRECT url have access to the target form.
the data_upload fucntion is the main entry point, and acts as a wrapper to upload the dataframe.

Required Args for the data_upload function:
    df: pd.DataFrame - DataFrame to upload
    form_name: str - Name of the form in Smartabase to upload to
    sb_username: str - Smartabase username
    sb_password: str - Smartabase password
    sb_url: str - Base URL of the Smartabase instance
    sb_app_id: str - Application ID for Smartabase API access
    verbose: bool - Whether to print progress messages

 Required Columns in DataFrame:
    "First Name" - First name of the user
    "Last Name" - Last name of the user
    "ID" - Unique identifier for the measurement/event 
         - this is different from the event-ID and allows for deduplication and merging
         - this should be either pulled from the api or created in a unique and replicable way
'''
# =========================
# EVENT PAYLOAD - TODO: customize per form
# =========================

def _build_event_payload(row, form_name):
    ''' fucntion to build the event payload for a single row of data, this is an EXAMPLE and should be customized per form '''
    pair_keys = [
        "ID",
        "Duration",
        "Session Type",
        "ACWR",
        "RMSSD",
        "HR Avg",
        "HR Peak",
        "TRIMP",
        "Movement Load",
        "Zone 1 (min)",
        "Zone 2 (min)",
        "Zone 3 (min)",
        "Zone 4 (min)",
        "Zone 5 (min)"
    ]
    
    pairs = [
        {"key": key, "value": str(row.get(key, ""))}
        for key in pair_keys
    ]
    
    return {
        "formName": form_name,
        "startDate": row.get("start_date", pd.Timestamp.now().strftime("%d/%m/%Y")),
        "startTime": row.get("start_time", ""),
        "finishDate": row.get("end_date", pd.Timestamp.now().strftime("%d/%m/%Y")),
        "finishTime": row.get("end_time", ""),
        "userId": {"userId": int(row["user_id"])},
        "rows": [
            {
                "row": 0,
                "pairs": pairs
            }
        ]
    }


# =========================
# PUBLIC ENTRY POINT - MAIN MEHTHOD
# =========================

def upload_dataframe(df: pd.DataFrame, form_name, sb_username, sb_password, sb_url, sb_app_id, verbose: bool = True) -> int:
    """
    Uploads form name data into Smartabase.
    NOTE: DataFrame must contain the correct that match the form in and must have columns:
        "First Name", "Last Name", "ID" where ID is a unique identifier for the measurement/event that allows for deduplication.
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
    user_map = get_usss_user_map(sb_username, sb_password, sb_url, sb_app_id)

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
    existing_ids = get_existing_measurement_ids(df["user_id"].tolist(), form_name, sb_username, sb_password, sb_app_id, sb_url)
    df = df[~df["ID"].isin(existing_ids)]

    if df.empty:
        if verbose:
            print("All rows already exist in Smartabase. Nothing to upload.")
        return 0

    # -------------------------
    # Upload
    # -------------------------
    url = f"{sb_url}/api/v1/eventimport?informat=json&format=json"

    success_count = 0

    for _, row in df.iterrows():
        payload = _build_event_payload(row, form_name)

        r = requests.post(
            url,
            headers=_sb_headers(sb_app_id),
            auth=_sb_auth(sb_username, sb_password),
            json=payload,
            timeout=30
        )

        if r.status_code == 200:
            success_count += 1
            if verbose:
                print(f"Uploaded Measurement: {row['First Name']} {row['Last Name']} (Session ID: {row['ID']})")
        else:
            print(f"FAILED ({row['ID']}): {r.status_code} - {r.text}")

    if verbose:
        print(f"Successfully uploaded {success_count} {form_name} events.")

    return success_count

# =========================
# AUTH / HEADERS
# =========================

def _sb_headers(sb_app_id):
    return {
        "X-APP-ID": sb_app_id,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

def _sb_auth(sb_username, sb_password):
    return HTTPBasicAuth(sb_username, sb_password)


def _chunk_list(values, chunk_size):
    for i in range(0, len(values), chunk_size):
        yield values[i:i + chunk_size]


def _post_with_retries(url, headers, auth, payload, timeout=60, max_attempts=3):
    """
    Retries transient request failures (network/5xx) with exponential backoff.
    """
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(
                url,
                headers=headers,
                auth=auth,
                json=payload,
                timeout=timeout
            )
            response.raise_for_status()
            return response
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            last_error = exc

            # Non-server errors should fail fast (bad auth, bad request, etc).
            if status is not None and status < 500:
                raise

            if attempt < max_attempts:
                time.sleep(2 ** (attempt - 1))
        except requests.RequestException as exc:
            last_error = exc
            if attempt < max_attempts:
                time.sleep(2 ** (attempt - 1))

    raise last_error

# =========================
# USERS (CORRECT ENDPOINT)
# =========================

def get_usss_user_map(sb_username, sb_password, sb_url, sb_app_id):
    """
    Uses /usersynchronise to retrieve all accessible users.

    Returns:
        dict[(first_name, last_name)] -> user_id
    """
    url = f"{sb_url}/api/v1/usersynchronise?informat=json&format=json"

    payload = {
        "lastSynchronisationTimeOnServer": 0,
        "paginate": True
    }

    user_map = {}

    while True:
        r = _post_with_retries(
            url,
            headers=_sb_headers(sb_app_id),
            auth=_sb_auth(sb_username, sb_password),
            payload=payload,
            timeout=60,
            max_attempts=3
        )
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


def _fetch_existing_measurement_ids_for_user_batch(user_ids, form_name, sb_username, sb_password, sb_app_id, sb_url):
    """
    Syncs events for a subset of users and returns discovered event IDs.
    """
    url = f"{sb_url}/api/v1/synchronise?informat=json&format=json"
    payload = {
        "formName": form_name,
        "lastSynchronisationTimeOnServer": 0,
        "userIds": user_ids,
        "paginate": True
    }

    existing_ids = set()

    while True:
        r = _post_with_retries(
            url,
            headers=_sb_headers(sb_app_id),
            auth=_sb_auth(sb_username, sb_password),
            payload=payload,
            timeout=60,
            max_attempts=3
        )
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
def get_existing_measurement_ids(user_ids, form_name, sb_username, sb_password, sb_app_id, sb_url):
    """
    Pulls existing form events
    and returns a set of measurement IDs already uploaded.
    """
    if not user_ids:
        return set()

    unique_user_ids = []
    for user_id in user_ids:
        if pd.notna(user_id):
            unique_user_ids.append(int(user_id))
    unique_user_ids = list(dict.fromkeys(unique_user_ids))

    existing_ids = set()
    user_batch_size = 25

    for batch in _chunk_list(unique_user_ids, user_batch_size):
        try:
            existing_ids.update(
                _fetch_existing_measurement_ids_for_user_batch(
                    batch,
                    form_name,
                    sb_username,
                    sb_password,
                    sb_app_id,
                    sb_url
                )
            )
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None

            # Fallback for intermittent/size-related server failures.
            if status is not None and status >= 500 and len(batch) > 1:
                print(
                    f"WARNING: Smartabase sync returned {status} for a user batch of "
                    f"{len(batch)}. Retrying one user at a time."
                )
                for user_id in batch:
                    try:
                        existing_ids.update(
                            _fetch_existing_measurement_ids_for_user_batch(
                                [user_id],
                                form_name,
                                sb_username,
                                sb_password,
                                sb_app_id,
                                sb_url
                            )
                        )
                    except requests.RequestException as single_exc:
                        print(f"WARNING: Could not dedupe existing events for user_id={user_id}: {single_exc}")
                continue

            raise
        except requests.RequestException as exc:
            # Keep the upload running even if dedup fetch fails unexpectedly.
            print(f"WARNING: Could not fetch existing event IDs for a user batch: {exc}")

    return existing_ids
