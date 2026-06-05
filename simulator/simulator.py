from flask import Flask, jsonify
import random
import uuid
from datetime import datetime, timezone
from faker import Faker

# Flask turns Python script into a server that listens for HTTP requests. 
# NiFi → HTTP GET → Flask → calls generate_post() → returns JSON → NiFi receives it


app = Flask(__name__)
fake = Faker()

# Define a list of sample hashtags to use in the generated tweets
HASHTAGS = [ 
    "#AI", "#Tech", "#Data", "#Python", "#Kafka",
    "#Spark", "#MachineLearning", "#BigData", "#Analytics",
    "#Engineering", "#Cloud", "#Streaming", "#OpenSource"
]

# Fixed pool of 20 user IDs to simulate repeat users.
# Using a fixed pool means some users will appear multiple times across posts 
# which is realistic and necessary for the "most active users" calculation in Phase 4.
USERS = [f"U{str(i).zfill(3)}" for i in range(1, 21)]

# Function that builds one post as a dictionary
def generate_post():
    
    # picks between 1 and 3 hashtags without repetition from the pool.
    selected_hashtags = random.sample(HASHTAGS, k=random.randint(1, 3))
    
    #  fake.sentence(nb_words=8) generates a realistic 8-word sentence as the post body the hashtags appended
    #  post_text combines the sentence with the hashtags so NiFi can extract them via regex in Phase 2
    post_text = fake.sentence(nb_words=8) + " " + " ".join(selected_hashtags)
    
    # assemble the full post object matching the JSON schema
    post = {
        "post_id": f"P{uuid.uuid4().hex[:8].upper()}",
        "user_id": random.choice(USERS),
        "text": post_text,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "likes": random.randint(0, 500),
        "retweets": random.randint(0, 200)
    }
    
    return post

# define the REST endpoint NiFi will poll
@app.route("/posts", methods=["GET"])
def get_post():
    
    
    post = generate_post() # generate one fresh post every time NiFi calls this endpoint
    

    return jsonify(post) # return it as a JSON HTTP response

# Step 6: function to generate and save 100+ sample posts to a JSON file
def generate_sample_data(n=150):
    import json
    
    posts = [generate_post() for _ in range(n)]
    
    with open("../data/sample_posts.json", "w") as f:
        json.dump(posts, f, indent=2)
    
    print(f"Generated {n} sample posts to data/sample_posts.json")
    
    # Entry point - generates sample data then starts the Flask server
if __name__ == "__main__":
    
    # generate sample posts file on startup
    generate_sample_data(150)
    
    # start Flask server on port 5000, accessible from other containers
    app.run(host="0.0.0.0", port=5000, debug=False)