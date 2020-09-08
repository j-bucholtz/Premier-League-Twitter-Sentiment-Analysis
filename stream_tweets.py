"""
Stream Tweets from the Twitter filtered stream API.
"""
import configparser
import json
import pprint
from typing import Dict
from urllib.parse import urlparse

import requests


def get_bearer_token() -> str:
    """Fetches bearer token from credentials config"""
    credentials = configparser.ConfigParser(interpolation=None)
    credentials.read("./config/credentials.ini")
    return credentials["twitter_api"]["bearer_token"]


def get_api_parameters() -> Dict[str, str]:
    """Get API parameters from api config"""
    api_parameters = configparser.ConfigParser(interpolation=None)
    api_parameters.read("./config/twitter_api_parameters.ini")
    return dict(api_parameters["stream_parameters"])


def build_headers(bearer_token: str) -> str:
    """Returns the headers with the bearer token included"""
    headers = {"Authorization": f"Bearer {bearer_token}"}
    return headers


def build_url(parameters: dict) -> str:
    """Returns a URL including Tweet fields, expansions, etc"""
    # TODO: Loop over entire dict and for each key create key=values
    # Use urllib.parse
    url = f"{parameters.get('endpoint')}?tweet.fields={parameters.get('tweet.fields')}"\
            f"&expansions={parameters.get('expansions')}"
    return url


def connect_to_endpoint(url, headers):
    """Stream Tweets from the URL"""
    response = requests.request("GET", url, headers=headers, stream=True)
    for tweet in response.iter_lines():
        if tweet:
            json_response = json.loads(tweet)
            pprint.pprint(json.dumps(json_response, sort_keys=True))
        if response.status_code != 200:
            raise Exception(f"Request returned an error: {response.status_code}, {response.text}")


def main():
    bearer_token = get_bearer_token()
    headers = build_headers(bearer_token)
    api_parameters = get_api_parameters()
    streaming_url = build_url(api_parameters)
    timeout = 0
    while True:
        connect_to_endpoint(streaming_url, headers)
        timeout += 1


if __name__ == "__main__":
    main()
