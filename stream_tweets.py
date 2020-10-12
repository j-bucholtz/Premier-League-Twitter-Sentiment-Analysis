"""
Stream Tweets from the Twitter filtered stream API.
"""
import configparser
import json
from typing import Dict, Tuple

import requests

CREDENTIALS_FILE = "./config/credentials.ini"
STREAM_PARAMETERS = "./config/twitter_api_parameters.ini"

class Authorization:
    """
    Retrieves the bearer_token and builds headers from a credentails_file.

    Args:
        credentails_file (str): Path to credentials file including Twitter Bearer token.

    Attributes:
        bearer_token (str): Twitter Bearer token
        headers (str): Headers used for Twitter API
    """

    def __init__(self, credentials_file : str):
        self._credentials_file = credentials_file
        self.bearer_token = self.get_bearer_token()
        self.headers = self.build_headers()

    def get_bearer_token(self) -> str:
        """Fetches bearer token from credentials config"""
        credentials = configparser.ConfigParser(interpolation=None)
        credentials.read(self._credentials_file)
        return credentials["twitter_api"]["bearer_token"]

    def build_headers(self) -> str:
        """Returns the headers with the bearer token included"""
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        return headers


def get_rules(headers) -> bool:
    """Get all filtered stream rules."""
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", headers=headers
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(headers, rules) -> bool:
    """Delete all filtered stream rules."""
    if rules is None or "data" not in rules:
        return False
    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))
    return True


def set_rules(headers) -> bool:
    """Set filtered stream rules."""
    sample_rules = [
        {"value": "context:12.731226203856637952 lang:en", "tag": "Man City"}
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return True


def get_api_parameters() -> Tuple[str, Dict[str, str]]:
    """Get API parameters from api config"""
    api_parameters = configparser.ConfigParser(interpolation=None)
    api_parameters.read(STREAM_PARAMETERS)
    endpoint = api_parameters["general"]["endpoint"]
    query_parameters = dict(api_parameters["query_parameters"])
    return (endpoint, query_parameters)


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
    authorization = Authorization(CREDENTIALS_FILE)

    # Delete and recreate all rules
    rules = get_rules(authorization.headers)
    delete_all_rules(authorization.headers, rules)
    set_rules(authorization.headers)

    api_parameters = get_api_parameters()
    connect_to_stream(api_parameters[0], authorization.headers, parameters=api_parameters[1])


if __name__ == "__main__":
    main()
