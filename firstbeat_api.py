import os
import time
import jwt
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

# =========================
# ENV + AUTH SETUP
# =========================
load_dotenv()

BASE_URL = "https://api.firstbeat.com/v1"

CONSUMER_ID = os.getenv("ID")
SHARED_SECRET = os.getenv("SHARED_SECRET")
API_KEY = os.getenv("API_KEY")

if not CONSUMER_ID or not SHARED_SECRET or not API_KEY:
    raise RuntimeError("Missing ID, SHARED_SECRET, or API_KEY in .env")

def generate_jwt():
    now = int(time.time())
    payload = {"iss": CONSUMER_ID, "iat": now, "exp": now + 300}
    token = jwt.encode(payload, SHARED_SECRET, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token

def auth_headers():
    return {
        "Authorization": f"Bearer {generate_jwt()}",
        "x-api-key": API_KEY,
        "Accept": "application/json"
    }

def last_x_days_utc_range(days_back):
    today_utc = datetime.now(timezone.utc).date()
    past_utc = today_utc - timedelta(days=days_back)

    to_time = datetime.combine(today_utc, datetime.min.time(), tzinfo=timezone.utc)
    from_time   = datetime.combine(past_utc, datetime.max.time(), tzinfo=timezone.utc)

    return (
        from_time.isoformat().replace("+00:00", "Z"),
        to_time.isoformat().replace("+00:00", "Z")
    )

# =========================
# HELPER FOR SAFE REQUESTS
# =========================
def test_endpoint(name, url, params=None, max_retries=5):
    print(f"\n--- {name} ---")
    headers = auth_headers()
    #print("Authorization header (first 60):", headers["Authorization"][:60])
    #print("x-api-key (first 8):", headers["x-api-key"][:8])

    for attempt in range(max_retries):
        r = requests.get(url, headers=headers, params=params)
        print(f"Status: {r.status_code}")
        if r.status_code == 202:
            print("Analysis in progress, retrying in 5s...")
            time.sleep(5)
            continue
        break

    print("Response (first 500 chars):")
    print(r.text[:500])
    return r

# =========================
# STEP 1 — ACCOUNTS
# =========================
accounts_resp = test_endpoint("Accounts", f"{BASE_URL}/sports/accounts")
if accounts_resp.status_code != 200:
    print("\n❌ Cannot access accounts — stop here.")
    exit()
accounts = accounts_resp.json().get("accounts", [])
if not accounts:
    print("\n⚠️ No accounts returned.")
    exit()
account_id = accounts[1]["accountId"] # 0 is APITEST and 1 is USSS
name = accounts[1]["name"]
print("Using accountId:", account_id, 'from:', name)

# =========================
# STEP 2 — TEAMS
# =========================
teams_resp = test_endpoint(
    "Teams",
    f"{BASE_URL}/sports/accounts/{account_id}/teams"
)
if teams_resp.status_code != 200:
    print("\n Cannot access teams — stop here.")
    exit()
teams = teams_resp.json().get("teams", [])
if not teams:
    print("\n⚠️ No teams returned.")
    exit()
team_id = teams[0]["teamId"]
print("Using teamId:", team_id)

# =========================
# STEP 3 — ATHLETES
# =========================
athletes_resp = test_endpoint(
    "Athletes",
    f"{BASE_URL}/sports/accounts/{account_id}/teams/{21426}/athletes"
)
athletes = athletes_resp.json().get("athletes", [])
print(f"Found {len(athletes)} athletes")

# =========================
# STEP 4 — SESSIONS (OPTIONAL DATE FILTER)
# =========================
from_time, to_time = last_x_days_utc_range(365)
print(team_id)
print(from_time, to_time)
sessions_resp = test_endpoint(
    "Sessions (all)",
    f"{BASE_URL}/sports/accounts/{account_id}/teams/{team_id}/sessions", #TODO: using a test ID here so change it when needed
    params={"fromTime": from_time, "toTime": to_time}
)
sessions = sessions_resp.json().get("sessions", [])
print(f"Found {len(sessions)} sessions")

# =========================
# STEP 5 — SESSION RESULTS
# =========================
for session in sessions:
    session_id = session["sessionId"]
    test_endpoint(
        f"Session Results for {session_id}",
        f"{BASE_URL}/sports/accounts/{account_id}/teams/{team_id}/sessions/{session_id}/results",
        params={"var": "acwr"} 
    )

print("\n=== DONE ===")
