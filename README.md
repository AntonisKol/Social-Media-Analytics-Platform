# Social Media Analytics Platform

Real-time social media analytics prototype using a mock REST API, Apache NiFi,
Kafka, Spark Structured Streaming, and Spark batch reporting.

Terraform is intentionally not used for this local demo. Docker Compose is the
infrastructure entry point.

## Architecture

```text
Mock Social API -> NiFi enrichment -> Kafka topic social_posts_raw
Kafka -> Spark Streaming sentiment windows -> console + parquet output
Kafka -> Spark Batch engagement reports -> CSV + PNG reports
```

Downloadable visual design: [`docs/pipeline_design.svg`](docs/pipeline_design.svg)
Presentation guide: [`docs/presentation_guide.md`](docs/presentation_guide.md)

## Repository layout

```text
NiFi Flow.png                  Screenshot of the NiFi canvas for presentation
data/                         Sample posts used for tests and demos
docs/pipeline_design.svg       Downloadable visual design of the full pipeline
infra/docker-compose.yml       Local NiFi/Kafka/Spark/simulator stack
nifi/social_media_flow.json    Source-controlled NiFi flow definition
scripts/create_kafka_topic.sh  Manual Kafka topic creation helper
simulator/                     Flask mock social API
spark/batch/                   Spark batch report job
spark/streaming/               Spark streaming sentiment job
tests/                         Pytest validation suite
```

## Task coverage

| Requirement | Where it is covered |
| --- | --- |
| Docker setup for NiFi, Kafka, and Spark | `infra/docker-compose.yml` |
| Mock social media API | `simulator/simulator.py` |
| 100+ sample records | `data/sample_posts.json` |
| NiFi API polling, enrichment, and Kafka publish flow | `nifi/social_media_flow.json` and `NiFi Flow.png` |
| Kafka topic for enriched posts | `social_posts_raw`, created by `kafka-init` or `scripts/create_kafka_topic.sh` |
| Real-time sentiment analysis | `spark/streaming/sentiment_streaming.py` |
| Two-minute hashtag sentiment windows | Streaming job console and parquet output |
| Batch top hashtags, active users, and engagement reports | `spark/batch/batch_analysis.py` |
| CSV reports and plots | `spark/batch/output` after the batch job runs |
| Validation tests | `tests/test_pipeline.py` |

## 1. Install Python dependencies

Use Python 3.10+. You also need Java available because Spark runs on the JVM.
Java 17 is a safe choice for Spark 3.5.x.

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

## 2. Start the local platform

```bash
docker compose -f infra/docker-compose.yml up -d --build
```

Useful UIs:

- Mock API: <http://localhost:5000/posts>
- NiFi: <http://localhost:8080/nifi> (`admin` / `adminpassword`)
- Spark master: <http://localhost:8081>
- Kafka host bootstrap: `localhost:9094`
- Kafka Docker-network bootstrap: `kafka:9092`

The Compose stack includes a `kafka-init` service that creates the topic
`social_posts_raw`. If you need to create or verify it manually, run:

```bash
./scripts/create_kafka_topic.sh
```

## 3. Configure NiFi

The version-controlled NiFi flow description lives at:

```text
nifi/social_media_flow.json
```

Use it as a build guide in the NiFi UI. The file is written to be readable for
the project, not as a one-click NiFi Registry import.

1. `Poll Mock Social API` (`InvokeHTTP`)
   - Method: `GET`
   - URL: `http://simulator:5000/posts`
   - Schedule: `5 sec`
2. `Extract JSON Fields` (`EvaluateJsonPath`)
   - Extracts `post_id`, `user_id`, `text`, `timestamp`, `likes`, `retweets`
     into attributes.
3. `Route Valid Posts` (`RouteOnAttribute`)
   - Routes `matched` flow files (valid JSON) to `Extract Hashtags`.
   - Routes `unmatched` flow files (missing required fields) to
     `PublishKafka_error` so bad records never reach Kafka.
4. `Extract Hashtags` (`ExtractText`)
   - Regex: `((?:#\w+\s*)+)` — captures all hashtags in one attribute.
5. `Enrich User Location` (`UpdateAttribute`)
   - Maps `user_id` to a city name (e.g. `U001` → `Athens`).
6. `Build Enriched JSON` (`ReplaceText`)
   - Rewrites the FlowFile content as a JSON string containing
     `post_id, user_id, text, timestamp, likes, retweets, location, hashtags`
     so all enriched fields are in the Kafka message body.
7. `Publish Enriched Posts to Kafka` (`PublishKafka_2_6`) — `PublishKafka_valid`
   - Bootstrap servers: `kafka:9092`
   - Topic: `social_posts_raw`
8. `PublishKafka_error` (`PublishKafka_2_6`)
   - Receives unmatched flow files from step 3 for dead-letter handling.

## 4. Run Spark streaming sentiment analysis

From the host machine, use the external Kafka listener:

```bash
rm -rf spark/streaming/checkpoint spark/streaming/output

KAFKA_BOOTSTRAP_SERVERS=localhost:9094 \
KAFKA_TOPIC=social_posts_raw \
STREAMING_OUTPUT_DIR=spark/streaming/output \
STREAMING_CHECKPOINT_DIR=spark/streaming/checkpoint \
STREAMING_TRIGGER_INTERVAL="20 seconds" \
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --py-files spark/shared.py \
  spark/streaming/sentiment_streaming.py
```

`STREAMING_TRIGGER_INTERVAL` is shortened for class demos. If you omit it, the
job uses the assignment-style 2-minute windows.

The streaming job writes:

- live windowed sentiment counts to the console
- parquet output to `spark/streaming/output`

## 5. Run Spark batch engagement reports

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9094 \
KAFKA_TOPIC=social_posts_raw \
BATCH_OUTPUT_DIR=spark/batch/output \
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --py-files spark/shared.py \
  spark/batch/batch_analysis.py
```

Outputs (written to a date-stamped subdirectory, e.g. `spark/batch/output/2026-06-12/`):

- `spark/batch/output/<date>/top_hashtags`
- `spark/batch/output/<date>/active_users`
- `spark/batch/output/<date>/engagement_by_hashtag`
- `spark/batch/output/<date>/posts_by_location`
- `spark/batch/output/<date>/top_hashtags.png`
- `spark/batch/output/<date>/active_users.png`
- `spark/batch/output/<date>/engagement_by_hashtag.png`
- `spark/batch/output/<date>/posts_by_location.png`

These files are generated locally when the batch job runs and are intentionally
not committed to Git.

## 6. Run tests

```bash
python3 -m pytest -q
```

The tests validate:

- sentiment label behavior
- simulator post schema
- sample data size and uniqueness
- every sample record's required fields and basic types
- hashtag extraction for single and multi-hashtag posts

## Configuration reference

| Variable | Default | Used by |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Spark batch/streaming |
| `KAFKA_TOPIC` | `social_posts_raw` | Spark batch/streaming/topic script |
| `BATCH_OUTPUT_DIR` | `spark/batch/output` | Spark batch |
| `STREAMING_OUTPUT_DIR` | `spark/streaming/output` | Spark streaming |
| `STREAMING_CHECKPOINT_DIR` | `spark/streaming/checkpoint` | Spark streaming |
| `STREAMING_TRIGGER_INTERVAL` | `2 minutes` | Spark streaming |
| `KAFKA_PARTITIONS` | `1` | Topic script |
| `KAFKA_REPLICATION_FACTOR` | `1` | Topic script |

## Troubleshooting

Check containers:

```bash
docker compose -f infra/docker-compose.yml ps
```

Check Kafka topic:

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic social_posts_raw
```

Consume a few records:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic social_posts_raw \
  --from-beginning \
  --max-messages 5
```

Stop the stack:

```bash
docker compose -f infra/docker-compose.yml down
```
