from textblob import TextBlob


def analyse_sentiment(text):
    """Classify text into the sentiment buckets used by the streaming job."""
    if not text:
        return "neutral"

    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0.1:
        return "positive"
    if polarity < -0.1:
        return "negative"
    return "neutral"
