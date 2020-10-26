"""
Stream Tweets from the Twitter filtered stream API.
"""
import configparser
import json
import os
from typing import Dict, List, Optional, Tuple, Union

import requests

STREAM_PARAMETERS = "./config/twitter_api_parameters.ini"
BEARER_TOKEN = "BEARER_TOKEN"


def _get_bearer_token() -> Optional[str]:
    """Gets the bearer token from an environment variable."""
    return os.environ.get(BEARER_TOKEN)


def build_headers() -> Dict[str, str]:
    """Returns the headers with the bearer token included."""
    bearer_token = _get_bearer_token()
    headers = {"Authorization": f"Bearer {bearer_token}"}
    return headers


def get_rules(headers : Dict[str, str]) -> Dict[str, Union[List[Dict], Dict]]:
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


def delete_all_rules(
    headers : Dict[str, str],
    rules : Dict[str, Union[List[Dict], Dict]]
) -> bool:
    """Delete all filtered stream rules."""
    if rules is None or "data" not in rules:
        return False
    ids = [rule["id"] for rule in rules["data"]]
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


def set_rules(headers : Dict[str, str]) -> bool:
    """Set filtered stream rules."""
    sample_rules = [
        {"value": "context:12.731226203856637952 lang:en", "tag": "Man City"},
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
    """Get the endpoint and API parameters from the api config."""
    api_parameters = configparser.ConfigParser(interpolation=None)
    api_parameters.read(STREAM_PARAMETERS)
    endpoint = api_parameters["general"]["endpoint"]
    query_parameters = dict(api_parameters["query_parameters"])
    return (endpoint, query_parameters)


def connect_to_stream(
    url : str,
    headers : Dict[str, str],
    parameters : Optional[Dict[str, str]] = None
) -> None:
    """
    Stream Tweets from the Twitter streams URL. Optionally using any
    specified parameters.
    """
    response = requests.get(url, params=parameters, headers=headers, stream=True)
    if response.status_code != 200:
        raise Exception(
            f"Request returned an error: {response.status_code}, {response.text}"
        )
    for tweet in response.iter_lines():
        if tweet:
            json_response = json.loads(tweet)
            print(json.dumps(json_response, indent=4, sort_keys=True))


def main() -> None:
    headers = build_headers()

    rules = get_rules(headers)
    delete_all_rules(headers, rules)
    set_rules(headers)

    endpoint, query_parameters = get_api_parameters()
    connect_to_stream(endpoint, headers, parameters=query_parameters)


if __name__ == "__main__":
    main()
