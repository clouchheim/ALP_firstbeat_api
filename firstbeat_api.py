import os
import time
import jwt
from tqdm import tqdm
import pandas as pd
import requests
from dotenv import load_dotenv
from teamworks_api import upload_dataframe
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# =========================
# ENV + AUTH SETUP + GLOBALS
# =========================
load_dotenv()

BASE_URL = "https://api.firstbeat.com/v1"

CONSUMER_ID = os.getenv("ID")
SHARED_SECRET = os.getenv("SHARED_SECRET")
API_KEY = os.getenv("API_KEY")

SB_BASE_URL = os.getenv("SB_BASE_URL")
SB_USERNAME = os.getenv("SB_USERNAME")
SB_PASSWORD = os.getenv("SB_PASSWORD")
SB_APP_ID   = os.getenv("SB_APP_ID", "firstbeat-sync")

TEAM_ID = 20168 # all MALP and WALP on Firstbeat
LAST_X_HOURS = 36 # looks back 6 hours but runs every hour, removes duplicates in R script
USSS_COACH_ID = '3-4925' # U.S. Ski and Snowboard id

# IF missing correct infomration (probably in .env file, raise error)
if not SB_USERNAME or not SB_PASSWORD:
    raise RuntimeError("Missing SB_USERNAME or SB_PASSWORD in environment")

if not CONSUMER_ID or not SHARED_SECRET or not API_KEY:
    raise RuntimeError("Missing ID, SHARED_SECRET, or API_KEY in .env")

# GENERATE valid jwt key for header
def generate_jwt():
    now = int(time.time())
    payload = {"iss": CONSUMER_ID, "iat": now, "exp": now + 300}
    token = jwt.encode(payload, SHARED_SECRET, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token

# define headers for all api calls
def auth_headers():
    return {
        "Authorization": f"Bearer {generate_jwt()}",
        "x-api-key": API_KEY,
        "Accept": "application/json"
    }

# GET time bounds (from x days back, to present moment)
def last_x_hours_range(hours_back):
    now_utc = datetime.now(timezone.utc)
    from_time = now_utc - timedelta(hours=hours_back)

    return (
        from_time.isoformat().replace("+00:00", "Z"),
        now_utc.isoformat().replace("+00:00", "Z")
    )

# =========================
# HELPER FOR API REQUESTS
# =========================
def _retry_after_seconds(response, fallback_seconds):
    retry_after = response.headers.get("Retry-After")
    if not retry_after:
        return fallback_seconds
    try:
        return max(int(retry_after), 1)
    except ValueError:
        return fallback_seconds


def test_endpoint(name, url, params=None, max_retries=5):
    print(f"\n--- {name} ---")
    #print("Authorization header (first 60):", headers["Authorization"][:60])
    #print("x-api-key (first 8):", headers["x-api-key"][:8])

    last_exception = None
    response = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=auth_headers(), params=params, timeout=60)
        except requests.RequestException as exc:
            last_exception = exc
            if attempt == max_retries:
                raise
            wait_seconds = min(2 ** attempt, 30)
            print(f"Request error ({exc}). Retrying in {wait_seconds}s...")
            time.sleep(wait_seconds)
            continue

        if response.status_code == 202:
            wait_seconds = 5
            if attempt == max_retries:
                return response
            print(f"Analysis in progress, retrying in {wait_seconds}s...")
            time.sleep(wait_seconds)
            continue

        if response.status_code == 429:
            wait_seconds = _retry_after_seconds(response, fallback_seconds=min(2 ** attempt, 60))
            if attempt == max_retries:
                return response
            print(f"Rate limited (429), retrying in {wait_seconds}s...")
            time.sleep(wait_seconds)
            continue

        if response.status_code >= 500:
            wait_seconds = min(2 ** attempt, 30)
            if attempt == max_retries:
                return response
            print(f"Server error ({response.status_code}), retrying in {wait_seconds}s...")
            time.sleep(wait_seconds)
            continue

        return response

    if response is not None:
        return response
    raise last_exception

def get_measurement_ids(athlete_id, from_time, to_time, name=''):
    measurement = test_endpoint( 
        f"Measurements (athlete {name} ({athlete_id}))", 
        f"{BASE_URL}/sports/accounts/{USSS_COACH_ID}/athletes/{athlete_id}/measurements/", 
        params={"fromTime": from_time, "toTime": to_time},
        max_retries=8
    )

    try:
        measurement.raise_for_status()
    except requests.RequestException as exc:
        print(f"WARNING: Failed to fetch measurements for {name} ({athlete_id}): {exc}")
        return []

    # Get ids of measurements to pull results
    measurement_ids = [
        m["measurementId"]
        for m in measurement.json().get("measurements", [])
        if "measurementId" in m
    ]

    #if len(measurement_ids) > 0:
        #print(f'\n-- Found {len(measurement_ids)} measurement for {name} --')

    return measurement_ids

def get_measurement_results(athlete_id, measurement_id):
    resp = test_endpoint(
        f"Measurement Results ({measurement_id}-{athlete_id})",
        f"{BASE_URL}/sports/accounts/{USSS_COACH_ID}/athletes/{athlete_id}/measurements/{measurement_id}/results",
        params={
            "format": "list",
            "var": "rmssd,acwr,heartRateAverage,heartRatePeak,heartRateAveragePercentage,zone1Time,zone2Time,zone3Time,zone4Time,zone5Time,trimp,quickRecoveryTestScore,movementLoad"
        },
        max_retries=8
    )

    resp.raise_for_status()
    return resp.json()

# =========================
# STEP 3 — ATHLETES
# =========================
athletes_resp = test_endpoint(
    "Athlete List",
    f"{BASE_URL}/sports/accounts/{USSS_COACH_ID}/teams/{TEAM_ID}/athletes"
)
athletes = athletes_resp.json().get("athletes", [])
athlete_names = {}
for athlete in athletes:
    athlete_names[athlete['athleteId']] = f"{athlete['firstName']} {athlete['lastName']}"
print(f"Found {len(athletes)} athletes")

# =========================
# STEP 4 — GET ATHLETE MEASUREMENTS (think of as a session)
# =========================

# get measurementIds for athelte sessions in last x days
from_time, to_time = last_x_hours_range(LAST_X_HOURS) # get time intervals for last day
measurements = {}
for athlete in tqdm(athletes, "Fetching Athlete Sessions"):
    athlete_id = athlete['athleteId']
    name = athlete_names[athlete_id]

    # Pull athlete 'measurements'
    measurement_ids = get_measurement_ids(athlete_id, from_time, to_time, name=name)
    if measurement_ids != []:
        measurements[athlete_id] = measurement_ids

# get results of the measuremnts 
rmssd = []
athlete_w_measurements = list(measurements.keys())
for athlete in athlete_w_measurements:
    print(f'--- Getting Measurements for {athlete_names[athlete]} ---')
    for measurement_id in measurements[athlete]:
        try:
            resp = get_measurement_results(athlete, measurement_id)
        except requests.RequestException as exc:
            print(f"WARNING: Skipping measurement {measurement_id}-{athlete} after retries: {exc}")
            continue

        resp['endTime'] = datetime.fromisoformat(resp['endTime'].replace("Z", ""))
        resp['startTime'] = datetime.fromisoformat(resp['startTime'].replace("Z", ""))

        # get variables
        variables = {
            v.get("name"): v.get("value")
            for v in resp.get("variables", [])
        }
        
        rmssd_value = variables.get("rmssd", "")
        acwr_value = variables.get("acwr", "")
        hr_avg = variables.get("heartRateAverage", "")
        hr_peak = variables.get("heartRatePeak", "")
        trimp = variables.get("trimp", "")
        movement_load = variables.get("movementLoad", "")
        zone1 = variables.get("zone1Time", 0)
        zone2 = variables.get("zone2Time", 0)
        zone3 = variables.get("zone3Time", 0)
        zone4 = variables.get("zone4Time", 0)
        zone5 = variables.get("zone5Time", 0)
        
        session = {
            'start_date': resp['startTime'].strftime("%d/%m/%Y"),
            'start_time': resp['startTime'].strftime("%I:%M %p").lstrip("0"),
            'end_date': resp['endTime'].strftime("%d/%m/%Y"),
            'end_time': resp['endTime'].strftime("%I:%M %p").lstrip("0"),
            'First Name': athlete_names[athlete].split()[0],
            'Last Name': athlete_names[athlete].split()[1],
            'Date': resp['endTime'].strftime("%d/%m/%Y"),
            'Time': resp['endTime'].strftime("%I:%M %p").lstrip("0"),
            'ID': f'{measurement_id}-{athlete}',
            'Session Type': resp['measurementType'],
            'RMSSD': rmssd_value,
            'ACWR': acwr_value,
            'HR Avg': hr_avg,
            'HR Peak': hr_peak,
            'TRIMP': trimp,
            'Movement Load': movement_load,
            'Zone 1 (min)': zone1,
            'Zone 2 (min)': zone2,
            'Zone 3 (min)': zone3,
            'Zone 4 (min)': zone4,
            'Zone 5 (min)': zone5
        }
        rmssd.append(session)

df = pd.DataFrame(rmssd)

if len(rmssd) == 0:
    print("WARNING: No measurement data found, no upload.")
else:
    df = pd.DataFrame(rmssd)
    print("Uploading Firstbeat data to Smartabase... using the teamworks_api module.\n")
    upload_dataframe(df, "Firstbeat Summary Stats", SB_USERNAME, SB_PASSWORD, SB_BASE_URL, SB_APP_ID, verbose=True)

print("\n=== DONE WITH FIRSTBEAT API===\n")
