"""
Phase 5: Pipeline tests
Covers:
1. Sentiment accuracy validation (TextBlob classifier)
2. Simulator output validation (schema, field types, location enrichment)
3. Sample data validation (100+ records, required fields)
"""

import pytest
import json
import sys
import os

from textblob import TextBlob


# the same sentiment function used in sentiment_streaming.py
def analyse_sentiment(text):
    if not text:
        return "neutral"
    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0.1:
        return "positive"
    elif polarity < -0.1:
        return "negative"
    else:
        return "neutral"


# ── 1. Sentiment accuracy tests ──────────────────────────────────────────────────

class TestSentimentClassifier:

    def test_positive_text_classified_correctly(self):
        assert analyse_sentiment("I love this amazing product! It is wonderful.") == "positive"

    def test_negative_text_classified_correctly(self):
        assert analyse_sentiment("This is terrible and awful. I hate it.") == "negative"

    def test_neutral_text_classified_correctly(self):
        assert analyse_sentiment("The meeting is on Monday at 9am.") == "neutral"

    def test_empty_string_returns_neutral(self):
        assert analyse_sentiment("") == "neutral"

    def test_none_returns_neutral(self):
        assert analyse_sentiment(None) == "neutral"

    def test_sentiment_returns_string(self):
        result = analyse_sentiment("Some random text with #Kafka #Spark")
        assert isinstance(result, str)

    def test_sentiment_only_valid_labels(self):
        texts = [
            "Amazing! I love this so much.",
            "This is absolutely terrible.",
            "The system processes data.",
            "",
            None
        ]
        valid_labels = {"positive", "negative", "neutral"}
        for text in texts:
            assert analyse_sentiment(text) in valid_labels

    def test_hashtag_text_gets_classified(self):
        result = analyse_sentiment("Great results today! #Kafka #Spark #AI")
        assert result in {"positive", "negative", "neutral"}


# ── 2. Simulator schema validation ───────────────────────────────────────────────

SIMULATOR_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "simulator")
sys.path.insert(0, SIMULATOR_PATH)
from simulator import generate_post

REQUIRED_FIELDS = {"post_id", "user_id", "text", "timestamp", "likes", "retweets", "location"}

VALID_CITIES = {
    "New York", "London", "Berlin", "Paris", "Tokyo",
    "Sydney", "Toronto", "Amsterdam", "Singapore", "Dubai",
    "Madrid", "Rome", "Chicago", "Los Angeles", "Sao Paulo",
    "Mumbai", "Seoul", "Mexico City", "Stockholm", "Cape Town"
}


class TestSimulatorSchema:

    def test_post_has_all_required_fields(self):
        post = generate_post()
        for field in REQUIRED_FIELDS:
            assert field in post, f"Missing field: {field}"

    def test_post_id_format(self):
        post = generate_post()
        assert post["post_id"].startswith("P")
        assert len(post["post_id"]) == 9

    def test_user_id_format(self):
        post = generate_post()
        assert post["user_id"].startswith("U")
        user_num = int(post["user_id"][1:])
        assert 1 <= user_num <= 20

    def test_likes_is_non_negative_integer(self):
        post = generate_post()
        assert isinstance(post["likes"], int)
        assert post["likes"] >= 0

    def test_retweets_is_non_negative_integer(self):
        post = generate_post()
        assert isinstance(post["retweets"], int)
        assert post["retweets"] >= 0

    def test_text_contains_at_least_one_hashtag(self):
        post = generate_post()
        assert "#" in post["text"]

    def test_location_is_non_empty_string(self):
        post = generate_post()
        assert isinstance(post["location"], str)
        assert len(post["location"]) > 0


# ── 3. Sample data file validation ───────────────────────────────────────────────

SAMPLE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "sample_posts.json")


class TestSampleData:

    def test_sample_file_exists(self):
        assert os.path.exists(SAMPLE_FILE), "sample_posts.json not found"

    def test_sample_file_has_100_plus_records(self):
        with open(SAMPLE_FILE) as f:
            posts = json.load(f)
        assert len(posts) >= 100, f"Only {len(posts)} records found, need 100+"

    def test_sample_records_have_required_fields(self):
        with open(SAMPLE_FILE) as f:
            posts = json.load(f)
        for i, post in enumerate(posts[:10]):
            for field in REQUIRED_FIELDS:
                assert field in post, f"Record {i} missing field: {field}"

    def test_sample_records_have_unique_post_ids(self):
        with open(SAMPLE_FILE) as f:
            posts = json.load(f)
        ids = [p["post_id"] for p in posts]
        assert len(ids) == len(set(ids)), "Duplicate post_ids found"
