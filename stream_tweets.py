"""
Stream Tweets from the Twitter filtered stream API.
"""
import configparser
import json
from typing import Dict
from typing import Tuple

import requests


def get_bearer_token() -> str:
    """Fetches bearer token from credentials config"""
    credentials = configparser.ConfigParser(interpolation=None)
    credentials.read("./config/credentials.ini")
    return credentials["twitter_api"]["bearer_token"]


def get_api_parameters() -> Tuple[str, Dict[str, str]]:
    """Get API parameters from api config"""
    api_parameters = configparser.ConfigParser(interpolation=None)
    api_parameters.read("./config/twitter_api_parameters.ini")
    endpoint = api_parameters["general"]["endpoint"]
    query_parameters = dict(api_parameters["query_parameters"])
    return (endpoint, query_parameters)


def build_headers(bearer_token: str) -> str:
    """Returns the headers with the bearer token included"""
    headers = {"Authorization": f"Bearer {bearer_token}"}
    return headers


def connect_to_stream(url, headers, parameters=None) -> None:
    """Stream Tweets from the URL using the parameters"""
    response = requests.get(url, params=parameters, headers=headers, stream=True)
    if response.status_code != 200:
        raise Exception(f"Request returned an error: {response.status_code}, {response.text}")
    for tweet in response.iter_lines():
        if tweet:
            json_response = json.loads(tweet)
            print(json.dumps(json_response, indent=4, sort_keys=True))

def main() -> None:
    bearer_token = get_bearer_token()
    headers = build_headers(bearer_token)
    api_parameters = get_api_parameters()
    timeout = 0
    while True:
        connect_to_stream(api_parameters[0], headers, parameters=api_parameters[1])
        timeout += 1


if __name__ == "__main__":
    main()
