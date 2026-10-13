# Social Media Analytics Platform

A local analytics pipeline that turns a stream of fake social posts into live sentiment trends and batch engagement reports.

Posts start at a mock API, pass through Apache NiFi for cleanup and enrichment, land in Kafka, and are analyzed by two Spark jobs: one that scores sentiment in near real time, and one that writes summary tables and charts.

```text
Mock Social API  ->  NiFi (validate, hashtags, city)  ->  Kafka topic social_posts_raw
                                                              |
                         +------------------------------------+------------------+
                         |                                                       |
              Spark streaming                                          Spark batch
              2-minute sentiment windows                               CSV reports and PNG charts
              console + Parquet
```

Visual design: [`docs/pipeline_design.svg`](docs/pipeline_design.svg). NiFi canvas screenshot: [`docs/NiFi Flow.png`](docs/NiFi%20Flow.png).

## What the app does

**Mock social API** (`simulator/simulator.py`). A Flask service. Each `GET /posts` returns one generated post with `post_id`, `user_id`, `text` (a sentence plus one to three hashtags), `timestamp`, `likes`, and `retweets`. On startup it also writes 150 sample posts to `data/sample_posts.json` for tests and offline checks.

**NiFi** (`nifi/social_media_flow.json`). Polls the mock API every 5 seconds, drops posts that are missing an id, extracts hashtags, maps each user id to a city (for example `U001` to Athens), and publishes the enriched JSON to Kafka. Posts that fail validation go to the dead-letter topic `social_posts_dead_letter`.

**Spark streaming** (`spark/streaming/sentiment_streaming.py`). Reads `social_posts_raw`, labels each post `positive`, `neutral`, or `negative` with TextBlob, and counts posts by hashtag and city inside 2-minute windows. Results print to the console and are written as Parquet under `spark/streaming/output`.

**Spark batch** (`spark/batch/batch_analysis.py`). Reads the same topic once and writes four reports, each as a CSV directory and a PNG chart, under `spark/batch/output/<date>/`:

- top 10 hashtags by post count
- top 10 most active users
- average engagement (likes + retweets) by hashtag
- post count and average engagement by city

## Repository layout

```text
data/                      Sample posts used by tests
docs/                      Pipeline diagram and NiFi screenshot
infra/docker-compose.yml   Local simulator, NiFi, Kafka, and Spark stack
nifi/                      Readable NiFi flow definition (build guide, not a registry import)
simulator/                 Flask mock social API
spark/batch/               Batch engagement reports
spark/streaming/           Streaming sentiment windows
spark/shared.py            Shared post schema and hashtag pattern
tests/                     Pytest checks for sentiment, hashtags, and sample data
```

## How to run it

You need Docker and Docker Compose. Python 3.10+ is only required for the tests. Spark and the mock API run inside containers, so you do not need Java or PySpark on the host.

Run every command from the repository root.

### 1. Start the stack

```bash
docker compose -f infra/docker-compose.yml up -d --build
```

The `kafka-init` service creates the topic `social_posts_raw` (1 partition, replication factor 1).

Once the containers are up:

| Service | Where |
| --- | --- |
| Mock API | <http://localhost:5000/posts> |
| NiFi | <http://localhost:8080/nifi> (`admin` / `adminpassword`) |
| Spark master UI | <http://localhost:8081> |
| Kafka from the host | `localhost:9094` |
| Kafka from other containers | `kafka:9092` |

Check that everything is running:

```bash
docker compose -f infra/docker-compose.yml ps
```

### 2. Build and start the NiFi flow

`nifi/social_media_flow.json` is a readable build guide. It is not a one-click NiFi Registry import. Recreate the processors in the NiFi UI, then start them. Spark has nothing to read until NiFi is publishing.

1. `Poll Mock Social API` (`InvokeHTTP`): `GET` `http://simulator:5000/posts`, schedule `5 sec`.
2. `Extract JSON Fields` (`EvaluateJsonPath`): copy `post_id`, `user_id`, `text`, `timestamp`, `likes`, and `retweets` into attributes.
3. `Route Valid Posts` (`RouteOnAttribute`): send posts that have both ids down the happy path. Send the rest to `Dead Letter Kafka` (topic `social_posts_dead_letter`).
4. `Extract Hashtags` (`ExtractText`): capture the hashtags from the post text.
5. `Enrich User Location` (`UpdateAttribute`): map `user_id` to a city.
6. `Build Enriched JSON` (`ReplaceText`): rewrite the flowfile as JSON with `post_id`, `user_id`, `text`, `timestamp`, `likes`, `retweets`, `location`, and `hashtags`.
7. `Publish Enriched Posts to Kafka` (`PublishKafka_2_6`): bootstrap `kafka:9092`, topic `social_posts_raw`.

Confirm records are arriving:

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic social_posts_raw

docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic social_posts_raw \
  --from-beginning \
  --max-messages 5
```

### 3. Run live sentiment analysis

The job runs on the Spark cluster so it can reach Kafka at `kafka:9092`. Output is written back to the host through the `spark/` volume mount.

```bash
rm -rf spark/streaming/checkpoint spark/streaming/output

docker exec \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e KAFKA_TOPIC=social_posts_raw \
  -e STREAMING_OUTPUT_DIR=/opt/spark-jobs/streaming/output \
  -e STREAMING_CHECKPOINT_DIR=/opt/spark-jobs/streaming/checkpoint \
  -e STREAMING_TRIGGER_INTERVAL="20 seconds" \
  spark-master \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    --py-files /opt/spark-jobs/shared.py \
    /opt/spark-jobs/streaming/sentiment_streaming.py
```

`STREAMING_TRIGGER_INTERVAL` defaults to `2 minutes`. The `20 seconds` value above makes a local demo move faster. Leave the container running to keep seeing new windows. Stop it with Ctrl+C. Parquet files land in `spark/streaming/output`.

### 4. Run the batch reports

Wait until Kafka has some posts, then run:

```bash
docker exec \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e KAFKA_TOPIC=social_posts_raw \
  -e BATCH_OUTPUT_DIR=/opt/spark-jobs/batch/output \
  spark-master \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    --py-files /opt/spark-jobs/shared.py \
    /opt/spark-jobs/batch/batch_analysis.py
```

Reports are written to `spark/batch/output/<today's date>/` and are not committed to Git:

- `top_hashtags` and `top_hashtags.png`
- `active_users` and `active_users.png`
- `engagement_by_hashtag` and `engagement_by_hashtag.png`
- `posts_by_location` and `posts_by_location.png`

### 5. Run the tests

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
python3 -m pytest -q
```

The tests check sentiment labels, hashtag extraction, the simulator post shape, and that `data/sample_posts.json` has unique, well-formed records.

### 6. Stop the stack

```bash
docker compose -f infra/docker-compose.yml down
```

## Configuration

| Variable | Default | Used by |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` inside the cluster, `localhost:9094` from the host | Spark batch and streaming |
| `KAFKA_TOPIC` | `social_posts_raw` | Spark batch and streaming |
| `BATCH_OUTPUT_DIR` | `spark/batch/output` | Spark batch |
| `STREAMING_OUTPUT_DIR` | `spark/streaming/output` | Spark streaming |
| `STREAMING_CHECKPOINT_DIR` | `spark/streaming/checkpoint` | Spark streaming |
| `STREAMING_TRIGGER_INTERVAL` | `2 minutes` | Spark streaming |
