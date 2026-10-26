import requests
import json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path = "./.env")

API_KEY = os.getenv("API_KEY")

CHANNEL_HANDLE = "MrBeast"

def get_channel_subscriber_count():

    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=statistics&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        #print(json.dumps(data,indent=4))

        channel_items =data['items'][0]

        channel_subscribercount = channel_items['statistics']['subscriberCount']

        print (channel_subscribercount)
    
    except requests.exceptions.RequestException as e:
        raise e

if __name__ == "__main__":
    print("The function get_channel_subscriber_count will be executed")
    get_channel_subscriber_count()
else:
    print("This function get_channel_subscriber_count will not be executed directly")