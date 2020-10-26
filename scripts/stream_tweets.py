"""
Stream Tweets from the Twitter filtered stream API.
"""
from context import TwitterStream

twitter_stream = TwitterStream()
twitter_stream.reset_all_rules()
twitter_stream.connect_to_stream()
