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

## Repository layout

```text
data/                         Sample posts used for tests and demos
infra/docker-compose.yml       Local NiFi/Kafka/Spark/simulator stack
nifi/social_media_flow.json    Source-controlled NiFi flow definition
scripts/create_kafka_topic.sh  Manual Kafka topic creation helper
simulator/                     Flask mock social API
spark/common/                  Shared sentiment and hashtag helpers
spark/batch/                   Spark batch report job
spark/streaming/               Spark streaming sentiment job
tests/                         Pytest validation suite
```

## 1. Install Python dependencies

Use Python 3.10+.

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
- NiFi: <http://localhost:8080/nifi>
- Spark master: <http://localhost:8081>
- Kafka host bootstrap: `localhost:9094`
- Kafka Docker-network bootstrap: `kafka:9092`

The Compose stack includes a `kafka-init` service that creates the topic
`social_posts_raw`. If you need to create or verify it manually, run:

```bash
./scripts/create_kafka_topic.sh
```

## 3. Configure NiFi

The version-controlled NiFi flow definition lives at:

```text
nifi/social_media_flow.json
```

In the NiFi UI, recreate or import the flow using these processor names and
settings from the JSON artifact:

1. `Poll Mock Social API` (`InvokeHTTP`)
   - Method: `GET`
   - URL: `http://simulator:5000/posts`
   - Schedule: `5 sec`
2. `Extract JSON Fields` (`EvaluateJsonPath`)
   - Extracts `post_id`, `user_id`, `text`, `timestamp`, `likes`, `retweets`
     into attributes.
3. `Extract Hashtags` (`ExtractText`)
   - Regex: `#[A-Za-z0-9_]+`
4. `Lookup User Location` (`LookupAttribute`)
   - Uses the mock user-location mapping from `nifi/social_media_flow.json`.
5. `Build Enriched JSON` (`AttributesToJSON`)
   - Writes `post_id,user_id,text,timestamp,likes,retweets,location,hashtags`
     back to the FlowFile content.
6. `Publish Enriched Posts to Kafka` (`PublishKafka_2_6`)
   - Bootstrap servers: `kafka:9092`
   - Topic: `social_posts_raw`

For a local demo, route failure relationships to `LogAttribute` or terminate
them after validating the flow.

## 4. Run Spark streaming sentiment analysis

From the host machine, use the external Kafka listener:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9094 \
KAFKA_TOPIC=social_posts_raw \
STREAMING_OUTPUT_DIR=spark/streaming/output \
STREAMING_CHECKPOINT_DIR=spark/streaming/checkpoint \
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark/streaming/sentiment_streaming.py
```

From inside the Docker network, use `kafka:9092`:

```bash
docker exec -it \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e KAFKA_TOPIC=social_posts_raw \
  spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  --conf spark.executorEnv.KAFKA_TOPIC=social_posts_raw \
  /opt/spark-jobs/streaming/sentiment_streaming.py
```

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
  spark/batch/batch_analysis.py
```

Outputs:

- `spark/batch/output/top_hashtags`
- `spark/batch/output/active_users`
- `spark/batch/output/engagement_by_hashtag`
- `spark/batch/output/top_hashtags.png`
- `spark/batch/output/active_users.png`
- `spark/batch/output/engagement_by_hashtag.png`

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
