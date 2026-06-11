# Tests confirm the fake posts, sentiment labels, sample data, and hashtag extraction still behave as expected.
import json
import os
import re

from textblob import TextBlob

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SIMULATOR_PATH = os.path.join(ROOT_DIR, "simulator")

import sys
sys.path.insert(0, SIMULATOR_PATH)

from simulator import generate_post

HASHTAG_PATTERN = r"#\w+"


def extract_hashtags(text):
    if not text:
        return []
    return re.findall(HASHTAG_PATTERN, text)


def analyse_sentiment(text):
    if not text:
        return "neutral"

    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0.1:
        return "positive"
    if polarity < -0.1:
        return "negative"
    return "neutral"


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

REQUIRED_FIELDS = {"post_id", "user_id", "text", "timestamp", "likes", "retweets"}


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



SAMPLE_FILE = os.path.join(ROOT_DIR, "data", "sample_posts.json")


class TestSampleData:

    def test_sample_file_exists(self):
        assert os.path.exists(SAMPLE_FILE), "sample_posts.json not found"

    def _load_posts(self):
        with open(SAMPLE_FILE, encoding="utf-8") as f:
            return json.load(f)

    def test_sample_file_has_100_plus_records(self):
        posts = self._load_posts()
        assert len(posts) >= 100, f"Only {len(posts)} records found, need 100+"

    def test_sample_records_have_required_fields(self):
        posts = self._load_posts()
        for i, post in enumerate(posts):
            for field in REQUIRED_FIELDS:
                assert field in post, f"Record {i} missing field: {field}"

    def test_sample_records_have_unique_post_ids(self):
        posts = self._load_posts()
        ids = [p["post_id"] for p in posts]
        assert len(ids) == len(set(ids)), "Duplicate post_ids found"

    def test_sample_records_have_valid_field_types(self):
        posts = self._load_posts()
        for i, post in enumerate(posts):
            assert isinstance(post["post_id"], str), f"Record {i} post_id is not a string"
            assert isinstance(post["user_id"], str), f"Record {i} user_id is not a string"
            assert isinstance(post["text"], str), f"Record {i} text is not a string"
            assert isinstance(post["timestamp"], str), f"Record {i} timestamp is not a string"
            assert isinstance(post["likes"], int), f"Record {i} likes is not an integer"
            assert isinstance(post["retweets"], int), f"Record {i} retweets is not an integer"

    def test_sample_records_have_extractable_hashtags(self):
        posts = self._load_posts()
        for i, post in enumerate(posts):
            hashtags = extract_hashtags(post["text"])
            assert hashtags, f"Record {i} has no extractable hashtags"
            assert all(tag.startswith("#") for tag in hashtags)


class TestHashtagExtraction:

    def test_extracts_multiple_hashtags_in_order(self):
        text = "Amazing launch today #AI #Tech #Spark"
        assert extract_hashtags(text) == ["#AI", "#Tech", "#Spark"]

    def test_returns_empty_list_when_no_hashtags_exist(self):
        assert extract_hashtags("No tags in this post") == []

    def test_handles_empty_or_none_text(self):
        assert extract_hashtags("") == []
        assert extract_hashtags(None) == []
