from flask import Flask, jsonify
import random
import uuid
from datetime import datetime, timezone
from faker import Faker
import json

app = Flask(__name__)
fake = Faker()

HASHTAGS = [
    "#AI", "#Tech", "#Data", "#Python", "#Kafka",
    "#Spark", "#MachineLearning", "#BigData", "#Analytics",
    "#Engineering", "#Cloud", "#Streaming", "#OpenSource"
]

USERS = [f"U{str(i).zfill(3)}" for i in range(1, 21)]

def generate_post():
    selected_hashtags = random.sample(HASHTAGS, k=random.randint(1, 3))
    post_text = fake.sentence(nb_words=8) + " " + " ".join(selected_hashtags)
    user_id = random.choice(USERS)

    return {
        "post_id": f"P{uuid.uuid4().hex[:8].upper()}",
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
    posts = [generate_post() for _ in range(n)]
    with open("/data/sample_posts.json", "w") as f:
        json.dump(posts, f, indent=2)
    print(f"Generated {n} sample posts to /data/sample_posts.json")

if __name__ == "__main__":
    generate_sample_data(150)
    app.run(host="0.0.0.0", port=5000, debug=False)
