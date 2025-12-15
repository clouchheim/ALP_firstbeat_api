import os
import time
import jwt
import requests
from dotenv import load_dotenv

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
    payload = {
        "iss": CONSUMER_ID,
        "iat": now,
        "exp": now + 300
    }

    token = jwt.encode(payload, SHARED_SECRET, algorithm="HS256")

    # 🔑 CRITICAL: normalize to str
    if isinstance(token, bytes):
        token = token.decode("utf-8")

    return token

token = generate_jwt()
print("JWT:", token)
print("JWT dots:", token.count("."))

def auth_headers():
    return {
        "Authorization": f"Bearer {generate_jwt()}",
        "x-api-key": API_KEY,
        "Accept": "application/json"
    }




# =========================
# HELPER FOR SAFE REQUESTS
# =========================

def test_endpoint(name, url, params=None):
    print(f"\n--- {name} ---")

    headers = auth_headers()

    # DEBUG — leave this in for now
    print("Authorization header (first 60):", headers["Authorization"][:60])
    print("x-api-key (first 8):", headers["x-api-key"][:8])

    r = requests.get(
        url,
        headers=headers,
        params=params
    )

    print("Status:", r.status_code)
    print("Response (first 500 chars):")
    print(r.text[:500])

    return r


# =========================
# STEP 1 — ACCOUNTS
# =========================

accounts_resp = test_endpoint(
    "Accounts",
    f"{BASE_URL}/sports/accounts"
)


if not accounts_resp or accounts_resp.status_code != 200:
    print("\n❌ Cannot access accounts — stop here.")
    exit()

accounts = accounts_resp.json().get("accounts", [])
if not accounts:
    print("\n⚠️ No accounts returned.")
    exit()

account_id = accounts[1]["accountId"]
print("\nUsing accountId:", account_id)


# =========================
# STEP 2 — ATHLETES
# =========================

test_endpoint(
    "Athletes",
    f"{BASE_URL}/sports/athletes",
    params={"accountId": account_id}
)


# =========================
# STEP 3 — SESSIONS (NO FILTERS)
# =========================

test_endpoint(
    "Sessions (no filters)",
    f"{BASE_URL}/sports/sessions",
    params={"accountId": account_id}
)


# =========================
# STEP 4 — SESSIONS (DATE FILTERED)
# =========================

test_endpoint(
    "Sessions (dated)",
    f"{BASE_URL}/sports/sessions",
    params={
        "accountId": account_id,
        "fromTime": "2025-01-01T00:00:00Z",
        "toTime":   "2025-12-31T23:59:59Z"
    }
)


# =========================
# STEP 5 — MEASUREMENTS (NO VARS)
# =========================

test_endpoint(
    "Measurements (no vars)",
    f"{BASE_URL}/sports/measurements",
    params={"accountId": account_id}
)


# =========================
# STEP 6 — RESULTS (NO VARS)
# =========================

test_endpoint(
    "Results (no vars)",
    f"{BASE_URL}/sports/results",
    params={"accountId": account_id}
)


# =========================
# STEP 7 — RESULTS (SAFE VAR)
# =========================

test_endpoint(
    "Results (trainingLoad)",
    f"{BASE_URL}/sports/results",
    params={
        "accountId": account_id,
        "var": "trainingLoad"
    }
)


# =========================
# STEP 8 — RESULTS (RMSSD)
# =========================

test_endpoint(
    "Results (rmssd)",
    f"{BASE_URL}/sports/results",
    params={
        "accountId": account_id,
        "var": "rmssd"
    }
)

print("\n=== DONE ===")
