"""
Stream Tweets from the Twitter filtered stream API.
"""
import configparser
import json
import os
from typing import Dict, List, Optional, Tuple, Union

import requests


class TwitterStream:
    """Stream Tweets from the Twitter filtered stream API."""
    STREAM_PARAMETERS = f"{os.path.dirname(__file__)}/config/twitter_api_parameters.ini"
    BEARER_TOKEN = "BEARER_TOKEN"

    def __init__(self) -> None:
        self.headers = self._build_headers()

    def _get_bearer_token(self) -> Optional[str]:
        """Gets the bearer token from an environment variable."""
        return os.environ.get(self.BEARER_TOKEN)

    def _build_headers(self) -> Dict[str, str]:
        """Returns the headers with the bearer token included."""
        bearer_token = self._get_bearer_token()
        headers = {"Authorization": f"Bearer {bearer_token}"}
        return headers

    def get_rules(self) -> Dict[str, Union[List[Dict], Dict]]:
        """Get all filtered stream rules."""
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream/rules", headers=self.headers
        )
        if response.status_code != 200:
            raise Exception(
                f"Cannot get rules (HTTP {response.status_code}): {response.text}"
            )
        print(json.dumps(response.json()))
        return response.json()

    def delete_all_rules(
        self,
        rules : Dict[str, Union[List[Dict], Dict]]
    ) -> bool:
        """Delete all filtered stream rules."""
        if rules is None or "data" not in rules:
            return False
        ids = [rule["id"] for rule in rules["data"]]
        payload = {"delete": {"ids": ids}}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            headers=self.headers,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                f"Cannot delete rules (HTTP {response.status_code}): {response.text}"
            )
        print(json.dumps(response.json()))
        return True

    def set_rules(self) -> bool:
        """Set filtered stream rules."""
        sample_rules = [
            {"value": "context:12.731226203856637952 lang:en", "tag": "Man City"},
        ]
        payload = {"add": sample_rules}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            headers=self.headers,
            json=payload,
        )
        if response.status_code != 201:
            raise Exception(
                f"Cannot add rules (HTTP {response.status_code}): {response.text}"
            )
        print(json.dumps(response.json()))
        return True

    def reset_all_rules(self) -> bool:
        """Deletes all stream rules and recreates them."""
        rules = self.get_rules()
        self.delete_all_rules(rules)
        self.set_rules()
        return True

    def get_api_parameters(self) -> Tuple[str, Dict[str, str]]:
        """Get the endpoint and API parameters from the api config."""
        api_parameters = configparser.ConfigParser(interpolation=None)
        api_parameters.read(self.STREAM_PARAMETERS)
        endpoint = api_parameters["general"]["endpoint"]
        query_parameters = dict(api_parameters["query_parameters"])
        return (endpoint, query_parameters)

    def connect_to_stream(self) -> None:
        """
        Stream Tweets from the Twitter streams URL. Optionally using any
        specified parameters.
        """
        endpoint, query_parameters = self.get_api_parameters()
        response = requests.get(
            endpoint,
            params=query_parameters,
            headers=self.headers,
            stream=True
        )
        if response.status_code != 200:
            raise Exception(
                f"Request returned an error: {response.status_code}, {response.text}"
            )
        for tweet in response.iter_lines():
            if tweet:
                json_response = json.loads(tweet)
                print(json.dumps(json_response, indent=4, sort_keys=True))
