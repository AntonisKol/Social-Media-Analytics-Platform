# ============================================================
# Verify three core areas of the project:
#
# 1. Sentiment Analysis
#    - Positive text is classified as positive.
#    - Negative text is classified as negative.
#    - Neutral text is classified as neutral.
#    - Empty or missing text is handled safely.
#
# 2. Hashtag Extraction
#    - Hashtags are correctly identified from post text.
#    - Multiple hashtags are extracted in order.
#    - Empty text or text without hashtags is handled correctly.
#
# 3. Simulator and Sample Data Validation
#    - Generated posts contain all required fields.
#    - IDs follow the expected format.
#    - Numeric fields contain valid values.
#    - Sample data file exists and contains valid records.
#    - Sample records are unique and structurally correct.
# ============================================================

import json
import os
import re

from textblob import TextBlob

# Resolve project paths so simulator and shared modules can
# be imported regardless of where tests are executed.
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SIMULATOR_PATH = os.path.join(ROOT_DIR, "simulator")
SPARK_PATH = os.path.join(ROOT_DIR, "spark")

import sys
sys.path.insert(0, SIMULATOR_PATH)
sys.path.insert(0, SPARK_PATH)

from simulator import generate_post
from shared import HASHTAG_PATTERN


def extract_hashtags(text):
    """
    Extract all hashtags from a text string.

    Returns:
        list[str]: Hashtags found in the order they appear.
        Returns an empty list if text is empty or None.
    """
    if not text:
        return []
    return re.findall(HASHTAG_PATTERN, text)


def analyse_sentiment(text):
    """
    Determine sentiment using TextBlob polarity scores.

    Rules:
        polarity > 0.1   -> positive
        polarity < -0.1  -> negative
        otherwise        -> neutral

    Empty or missing text is treated as neutral.
    """
    if not text:
        return "neutral"

    polarity = TextBlob(text).sentiment.polarity

    if polarity > 0.1:
        return "positive"

    if polarity < -0.1:
        return "negative"

    return "neutral"


class TestSentimentClassifier:
    """
    Verify that sentiment classification produces valid and
    expected results for a variety of text inputs.
    """

    def test_positive_text_classified_correctly(self):
        # Positive language should be classified as positive.
        assert analyse_sentiment(
            "I love this amazing product! It is wonderful."
        ) == "positive"

    def test_negative_text_classified_correctly(self):
        # Negative language should be classified as negative.
        assert analyse_sentiment(
            "This is terrible and awful. I hate it."
        ) == "negative"

    def test_neutral_text_classified_correctly(self):
        # Informational text should be classified as neutral.
        assert analyse_sentiment(
            "The meeting is on Monday at 9am."
        ) == "neutral"

    def test_empty_string_returns_neutral(self):
        # Empty text should safely return neutral.
        assert analyse_sentiment("") == "neutral"

    def test_none_returns_neutral(self):
        # Missing text should safely return neutral.
        assert analyse_sentiment(None) == "neutral"

    def test_sentiment_returns_string(self):
        # Sentiment result should always be a string label.
        result = analyse_sentiment(
            "Some random text with #Kafka #Spark"
        )
        assert isinstance(result, str)

    def test_sentiment_only_valid_labels(self):
        # Sentiment must always be one of the allowed labels.
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
        # Presence of hashtags should not break classification.
        result = analyse_sentiment(
            "Great results today! #Kafka #Spark #AI"
        )
        assert result in {"positive", "negative", "neutral"}


# Fields required in every generated or stored post.
REQUIRED_FIELDS = {
    "post_id",
    "user_id",
    "text",
    "timestamp",
    "likes",
    "retweets"
}


class TestSimulatorSchema:
    """
    Validate the structure and format of posts generated
    by the simulator.
    """

    def test_post_has_all_required_fields(self):
        # Every generated post must contain all required fields.
        post = generate_post()

        for field in REQUIRED_FIELDS:
            assert field in post, f"Missing field: {field}"

    def test_post_id_format(self):
        # Post IDs should start with 'P' and match expected length.
        post = generate_post()

        assert post["post_id"].startswith("P")
        assert len(post["post_id"]) == 9

    def test_user_id_format(self):
        # User IDs should start with 'U' and reference valid users.
        post = generate_post()

        assert post["user_id"].startswith("U")

        user_num = int(post["user_id"][1:])
        assert 1 <= user_num <= 20

    def test_likes_is_non_negative_integer(self):
        # Like count should be a whole number >= 0.
        post = generate_post()

        assert isinstance(post["likes"], int)
        assert post["likes"] >= 0

    def test_retweets_is_non_negative_integer(self):
        # Retweet count should be a whole number >= 0.
        post = generate_post()

        assert isinstance(post["retweets"], int)
        assert post["retweets"] >= 0

    def test_text_contains_at_least_one_hashtag(self):
        # Generated posts are expected to include hashtags.
        post = generate_post()

        assert "#" in post["text"]


# Location of the sample dataset used for testing and demos.
SAMPLE_FILE = os.path.join(ROOT_DIR, "data", "sample_posts.json")


class TestSampleData:
    """
    Validate the integrity and quality of sample_posts.json.

    Checks:
    - File exists
    - Contains sufficient records
    - Required fields are present
    - IDs are unique
    - Data types are correct
    - Hashtags can be extracted
    """

    def test_sample_file_exists(self):
        # Sample dataset should be present in the expected location.
        assert os.path.exists(
            SAMPLE_FILE
        ), "sample_posts.json not found"

    def _load_posts(self):
        # Helper function to load all sample posts.
        with open(SAMPLE_FILE, encoding="utf-8") as f:
            return json.load(f)

    def test_sample_file_has_100_plus_records(self):
        # Dataset should contain at least 100 records.
        posts = self._load_posts()

        assert len(posts) >= 100, (
            f"Only {len(posts)} records found, need 100+"
        )

    def test_sample_records_have_required_fields(self):
        # Every sample record must contain required fields.
        posts = self._load_posts()

        for i, post in enumerate(posts):
            for field in REQUIRED_FIELDS:
                assert field in post, (
                    f"Record {i} missing field: {field}"
                )

    def test_sample_records_have_unique_post_ids(self):
        # Duplicate post IDs should not exist.
        posts = self._load_posts()

        ids = [p["post_id"] for p in posts]

        assert len(ids) == len(set(ids)), (
            "Duplicate post_ids found"
        )

    def test_sample_records_have_valid_field_types(self):
        # Ensure fields are stored using expected data types.
        posts = self._load_posts()

        for i, post in enumerate(posts):
            assert isinstance(post["post_id"], str), (
                f"Record {i} post_id is not a string"
            )
            assert isinstance(post["user_id"], str), (
                f"Record {i} user_id is not a string"
            )
            assert isinstance(post["text"], str), (
                f"Record {i} text is not a string"
            )
            assert isinstance(post["timestamp"], str), (
                f"Record {i} timestamp is not a string"
            )
            assert isinstance(post["likes"], int), (
                f"Record {i} likes is not an integer"
            )
            assert isinstance(post["retweets"], int), (
                f"Record {i} retweets is not an integer"
            )

    def test_sample_records_have_extractable_hashtags(self):
        # Every sample post should contain at least one hashtag.
        posts = self._load_posts()

        for i, post in enumerate(posts):
            hashtags = extract_hashtags(post["text"])

            assert hashtags, (
                f"Record {i} has no extractable hashtags"
            )

            assert all(
                tag.startswith("#") for tag in hashtags
            )


class TestHashtagExtraction:
    """
    Verify hashtag extraction behavior for common
    social-media posting scenarios.
    """

    def test_extracts_multiple_hashtags_in_order(self):
        # Multiple hashtags should be extracted in appearance order.
        text = "Amazing launch today #AI #Tech #Spark"

        assert extract_hashtags(text) == [
            "#AI",
            "#Tech",
            "#Spark"
        ]

    def test_returns_empty_list_when_no_hashtags_exist(self):
        # Posts without hashtags should return an empty list.
        assert extract_hashtags(
            "No tags in this post"
        ) == []

    def test_handles_empty_or_none_text(self):
        # Empty or missing text should safely return no hashtags.
        assert extract_hashtags("") == []
        assert extract_hashtags(None) == []