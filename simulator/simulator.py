from flask import Flask, jsonify
import random
import uuid
from datetime import datetime, timezone
from faker import Faker
import json

# Flask turns this script into an HTTP server
# NiFi polls GET /posts → Flask calls generate_post() → returns one JSON post
app = Flask(__name__)
fake = Faker()

# hashtag pool matching the business context: tech/data topics
HASHTAGS = [
    "#AI", "#Tech", "#Data", "#Python", "#Kafka",
    "#Spark", "#MachineLearning", "#BigData", "#Analytics",
    "#Engineering", "#Cloud", "#Streaming", "#OpenSource"
]

# fixed pool of 20 user IDs ensures repeat users appear across posts
# this is required for the Phase 4 "most active users" analysis to be meaningful
USERS = [f"U{str(i).zfill(3)}" for i in range(1, 21)]

# mock location lookup: maps each user ID to a fixed city
# satisfies Phase 2 requirement: "Enrich data with user location (mock lookup)"
USER_LOCATIONS = {
    "U001": "New York", "U002": "London", "U003": "Berlin",
    "U004": "Paris", "U005": "Tokyo", "U006": "Sydney",
    "U007": "Toronto", "U008": "Amsterdam", "U009": "Singapore",
    "U010": "Dubai", "U011": "Madrid", "U012": "Rome",
    "U013": "Chicago", "U014": "Los Angeles", "U015": "São Paulo",
    "U016": "Mumbai", "U017": "Seoul", "U018": "Mexico City",
    "U019": "Stockholm", "U020": "Cape Town"
}

def generate_post():
    # pick 1–3 unique hashtags from the pool for this post
    selected_hashtags = random.sample(HASHTAGS, k=random.randint(1, 3))

    # combine a realistic sentence with hashtags
    # hashtags are appended so NiFi can extract them via regex (Phase 2)
    post_text = fake.sentence(nb_words=8) + " " + " ".join(selected_hashtags)

    # pick a user and look up their mock location
    user_id = random.choice(USERS)
    location = USER_LOCATIONS[user_id]

    # assemble post matching the required JSON schema from the task spec
    # location field added here as the mock enrichment required by Phase 2
    return {
        "post_id": f"P{uuid.uuid4().hex[:8].upper()}",
        "user_id": user_id,
        "text": post_text,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "likes": random.randint(0, 500),
        "retweets": random.randint(0, 200),
        "location": location
    }

# REST endpoint NiFi polls continuously to get new posts
@app.route("/posts", methods=["GET"])
def get_post():
    return jsonify(generate_post())

def generate_sample_data(n=150):
    # generates n sample posts and saves to file
    # satisfies Phase 1 requirement: 100+ test records
    posts = [generate_post() for _ in range(n)]
    with open("/data/sample_posts.json", "w") as f:
        json.dump(posts, f, indent=2)
    print(f"Generated {n} sample posts to /data/sample_posts.json")

if __name__ == "__main__":
    # generate 150 sample posts on startup (satisfies 100+ test records requirement)
    generate_sample_data(150)

    # start server accessible from other Docker containers on port 5000
    app.run(host="0.0.0.0", port=5000, debug=False)
