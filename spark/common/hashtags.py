import re


HASHTAG_PATTERN = r"#\w+"


def extract_hashtags(text):
    if not text:
        return []
    return re.findall(HASHTAG_PATTERN, text)
