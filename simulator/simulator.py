from flask import Flask, jsonify
import random
import uuid
from datetime import datetime, timezone
from faker import Faker # used to generate realistic random sentence text
import json

app = Flask(__name__)
fake = Faker()  # used to generate realistic random sentence text

HASHTAGS = [
    "#AI", "#Tech", "#Data", "#Python", "#Kafka",
    "#Spark", "#MachineLearning", "#BigData", "#Analytics",
    "#Engineering", "#Cloud", "#Streaming", "#OpenSource"
]

USERS = [f"U{str(i).zfill(3)}" for i in range(1, 21)]

def generate_post():
    #  Generates a random social media post.
    selected_hashtags = random.sample(HASHTAGS, k=random.randint(1, 3))
    post_text = fake.sentence(nb_words=8) + " " + " ".join(selected_hashtags)
    user_id = random.choice(USERS)

    return {
        "post_id": f"P{uuid.uuid4().hex[:8].upper()}",  # short unique ID
        "user_id": user_id,
        "text": post_text,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "likes": random.randint(0, 500),
        "retweets": random.randint(0, 200)
    }

@app.route("/posts", methods=["GET"])
def get_post():
    return jsonify(generate_post())

def generate_sample_data(n=150):
    # It pre-populates a static dataset so the rest of the pipeline can be tested without the Flask service running. 
    # Instead of needing a live HTTP endpoint to get posts one at a time, the Kafka producer can just read from the JSON file and publish everything in bulk — useful for local dev, demos, or running the batch/streaming jobs when the simulator container isn't up.
    posts = [generate_post() for _ in range(n)]
    with open("/data/sample_posts.json", "w") as f:
        json.dump(posts, f, indent=2)
    print(f"Generated {n} sample posts to /data/sample_posts.json")

if __name__ == "__main__":
    generate_sample_data(150)  # seed the JSON file on startup
    app.run(host="0.0.0.0", port=5000, debug=False)