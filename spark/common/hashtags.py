import re


HASHTAG_PATTERN = r"#\w+"


def extract_hashtags(text):
    """Return every hashtag in a post body in original order."""
    if not text:
        return []
    return re.findall(HASHTAG_PATTERN, text)
