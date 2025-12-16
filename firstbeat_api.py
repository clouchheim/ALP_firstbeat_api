import os
import time
import jwt
from tqdm import tqdm
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

# =========================
# ENV + AUTH SETUP + GLOBALS
# =========================
load_dotenv()

BASE_URL = "https://api.firstbeat.com/v1"

CONSUMER_ID = os.getenv("ID")
SHARED_SECRET = os.getenv("SHARED_SECRET")
API_KEY = os.getenv("API_KEY")

TEAM_ID = 20168 # all MALP and WALP on Firstbeat
LAST_X_DAYS = 1 # just from time of run back 24 hours (NOTE: data must be loaded into firstbeat cloud)
USSS_COACH_ID = '3-4925' # U.S. Ski and Snowboard id

# IF missing correct infomration (probably in .env file, raise error)
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
def last_x_days_utc_range(days_back):
    today_utc = datetime.now(timezone.utc).date()
    past_utc = today_utc - timedelta(days=days_back)

    from_time = datetime.combine(past_utc, datetime.min.time(), tzinfo=timezone.utc)
    to_time   = datetime.combine(today_utc, datetime.max.time(), tzinfo=timezone.utc)

    return (
        from_time.isoformat().replace("+00:00", "Z"),
        to_time.isoformat().replace("+00:00", "Z")
    )

# =========================
# HELPER FOR API REQUESTS
# =========================
def test_endpoint(name, url, params=None, max_retries=5):
    #print(f"\n--- {name} ---")
    headers = auth_headers()
    #print("Authorization header (first 60):", headers["Authorization"][:60])
    #print("x-api-key (first 8):", headers["x-api-key"][:8])

    for attempt in range(max_retries):
        r = requests.get(url, headers=headers, params=params)
        #print(f"Status: {r.status_code}")
        if r.status_code == 202:
            print("Analysis in progress, retrying in 5s...")
            time.sleep(5)
            continue
        break
    
    return r

def get_measurement_ids(athlete_id, from_time, to_time, name=''):
    measurement = test_endpoint( 
        f"Measurements (athlete {name} ({athlete_id}))", 
        f"{BASE_URL}/sports/accounts/{USSS_COACH_ID}/athletes/{athlete_id}/measurements/", 
        params={"fromTime": from_time, "toTime": to_time})

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
            "var": "rmssd,acwr"
        }
    )

    resp.raise_for_status()
    return resp.json()

# =========================
# STEP 1 — ACCOUNTS
# =========================
'''accounts_resp = test_endpoint("Accounts", f"{BASE_URL}/sports/accounts")
if accounts_resp.status_code != 200:
    print("\n❌ Cannot access accounts — stop here.")
    exit()
coach_accounts = accounts_resp.json().get("accounts", [])
print(coach_accounts) 
if not coach_accounts:
    print("\n⚠️ No accounts returned.")
    exit()'''

# =========================
# STEP 2 — TEAMS - right now commented out because I have my hard coded TEAM_ID
# =========================
'''teams_resp = test_endpoint(
    "Teams",
    f"{BASE_URL}/sports/accounts/{USSS_COACH_ID}/teams"
)
if teams_resp.status_code != 200:
    print("\n Cannot access teams — stop here.")
    exit()
teams = teams_resp.json().get("teams", [])
print(teams)
if not teams:
    print("\n⚠️ No teams returned.")
    exit()'''

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
from_time, to_time = last_x_days_utc_range(LAST_X_DAYS) # get time intervals for last day
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
        resp = get_measurement_results(athlete, measurement_id)
        resp['endTime'] = datetime.fromisoformat(resp['endTime'].replace("Z", ""))
        print(resp['endTime'])
        print(f"ID: {measurement_id}-{athlete}\nTime: {resp['endTime']}\nType: {resp['measurementType']} \nRMSSD: {resp['variables'][0]['value']}\nACWR: {resp['variables'][1]['value']}\n")
        session = {
            'Firstname': athlete_names[athlete].split()[0],
            'Lastname': athlete_names[athlete].split()[1],
            'Date': str(resp['endTime']).split()[0],
            'Time': str(resp['endTime']).split()[1],
            'ID':f'{measurement_id}-{athlete}' , 
            'Session Type': resp['measurementType'],
            'RMSSD': resp['variables'][0]['value'], 
            'ACWR': resp['variables'][1]['value']
        }
        rmssd.append(session)

df = pd.DataFrame(rmssd)
print(df)

print("\n=== DONE WITH FIRSTBEAT API===\n")

