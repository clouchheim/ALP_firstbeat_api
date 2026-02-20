import requests
import pandas as pd
from requests.auth import HTTPBasicAuth

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

import json, hashlib, time, os, random
from datetime import datetime, timezone

def _redact(d, keys=("authorization", "password", "token")):
    if not isinstance(d, dict):
        return d
    out = {}
    for k, v in d.items():
        if any(x in k.lower() for x in keys):
            out[k] = "***REDACTED***"
        else:
            out[k] = v
    return out

def _payload_summary(payload):
    # Summarize “shape” without dumping everything
    if payload is None:
        return {"type": "None"}
    if isinstance(payload, (str, bytes)):
        return {"type": type(payload).__name__, "len": len(payload)}
    if isinstance(payload, dict):
        return {
            "type": "dict",
            "keys": sorted(list(payload.keys()))[:50],
            "n_keys": len(payload),
        }
    if isinstance(payload, list):
        return {
            "type": "list",
            "len": len(payload),
            "elem_types": sorted({type(x).__name__ for x in payload})[:20],
        }
    return {"type": type(payload).__name__}

def _stable_hash(obj) -> str:
    try:
        s = json.dumps(obj, sort_keys=True, default=str, ensure_ascii=True)
    except Exception:
        s = repr(obj)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

def log_request_response(tag, method, url, params=None, headers=None, payload=None, response=None, extra=None):
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "tag": tag,
        "method": method,
        "url": url,
        "params": params or {},
        "headers": _redact(headers or {}),
        "payload_summary": _payload_summary(payload),
        "payload_hash": _stable_hash(payload),
        "extra": extra or {},
    }
    if response is not None:
        record.update({
            "status_code": getattr(response, "status_code", None),
            "response_headers": _redact(dict(getattr(response, "headers", {}) or {})),
            "response_len": len(getattr(response, "text", "") or ""),
            "response_preview": (getattr(response, "text", "") or "")[:800],
        })

    # GitHub Actions-friendly: single-line JSON log
    print("SB_DIAG " + json.dumps(record, separators=(",", ":"), ensure_ascii=True))

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
        r = requests.post(
            url,
            headers=_sb_headers(sb_app_id),
            auth=_sb_auth(sb_username, sb_password),
            json=payload,
            timeout=60
        )
        
        if r.status_code >= 400:
            print("URL:", r.url)
            print("STATUS:", r.status_code)
            print("RESPONSE:", r.text[:2000])
            print("HEADERS:", dict(r.headers))
        
        r.raise_for_status()
        data = r.json()

        r = requests.post(url, params=params, headers=headers, json=payload)
        r.raise_for_status()

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

def get_existing_measurement_ids(user_ids, form_name, sb_username, sb_password, sb_app_id, sb_url):
    """
    Pulls existing form events
    and returns a set of measurement IDs already uploaded.
    """
    if not user_ids:
        return set()

    url = f"{sb_url}/api/v1/synchronise?informat=json&format=json"

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
            headers=_sb_headers(sb_app_id),
            auth=_sb_auth(sb_username, sb_password),
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
