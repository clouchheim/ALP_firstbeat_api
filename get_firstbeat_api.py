import time
import json
import jwt # pyjwt
import requests
from dotenv import load_dotenv
import os


BASE_URL = "https://api.firstbeat.com/v1"
API_KEY = ''

# put your creditials in a .env file
# echo "SHARED_SECRET=your_shared_secret_here" > .env
# echo "ID=your_consumer_id_here" >> .env
# call cat .env in command line to verify it's there
# do not push your .env file to github or any public repository, 
# this would expose the information get the API key

load_dotenv()
CONSUMER_ID: str = os.getenv("ID")
SHARED_SECRET: str = os.getenv("SHARED_SECRET")

def generate_jwt_token(shared_secret: str, consumer_id: str) -> str:    
    """  
    JWT token is valid for five minutes so it's valid approach to generate a new token for each API query.
    """

    secret: str = shared_secret
    now = int(time.time())
    expires = now + 300
    payload = {"iss": consumer_id, "iat": now, "exp": expires}

    return jwt.encode(payload, secret, algorithm='HS256')

def get_api_key():
    """    
    You need to include a JWT token in the query to create an API key. The API key only created once, so same repsonse in subsequent calls.

    As a response you get:

    {"apikey":"YOUR API KEY HERE"}

    """
    return requests.get(f"{BASE_URL}/account/api-key", headers=generate_headers()).json()["apikey"]

def generate_headers():

    """
    You need to include JWT token and API key in all API request excluding API key generation and API consumer creation (registration).

    Example headers:
        Authorization: Bearer eyJ0eXAiOiJKV1QiLCJ ...
        x-api-key: tQdt8RfzA....
    """

    query_headers = {

        "Authorization": "Bearer " + generate_jwt_token(SHARED_SECRET, CONSUMER_ID),
        "x-api-key": API_KEY,
    }

    return query_headers

def get_accounts():

    """Get accounts assigned to the API consumer

    One or more accounts can be assigned to one API consumer.

    Account can be accessed via Sports Cloud API if:

        1) Firstbeat support has granted access for your API consumer for the specific account(s)
        2) Account owner (for example team coach) has granted access to their account data

    Use the accountId in the subsequent queries to work with the selected account.

    Example response:

    {
        "accounts": [
            {
                "accountId": "3-99999",
                "name": "FC Firstbeat",
                "authorizedBy": {
                    "coachId": 12345
                }
            },
            {
                "accountId": "3-99998",
                "name": "Firstbeat Ice Hockey Team",
                "authorizedBy": {
                    "coachId": 67890
                }
            },
            {
                "accountId": "3-99997",
                "name": "FC Firstbeat Juniors",
                "authorizedBy": {
                    "coachId": 99999
                }
            }
        ]
    }

    """

    return requests.get(f"{BASE_URL}/sports/accounts/", headers=generate_headers())

# ===

def main():
   
    global API_KEY
   
    if not CONSUMER_ID or not SHARED_SECRET:
        print("Please set CONSUMER_ID and SHARED_SECRET")
        return

    print(f'JWT token: {generate_jwt_token(shared_secret=SHARED_SECRET, consumer_id=CONSUMER_ID)}')
   
    API_KEY = get_api_key()
    print(f'apikey: {API_KEY}')

    accounts = get_accounts()
    print(accounts)
    print(json.dumps(accounts.json(), indent=2))

if __name__ == "__main__":
    main()