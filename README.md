# kafka_module_1

## Environment Variables

- `CONSUMER_GROUP_ID` — Kafka consumer group id. Must be a positive integer.
- `TOPIC_NAME` — Kafka topic name to consume from or produce to.
- `BOOTSTRAP_SERVER` — Kafka broker address, for example `kafka:9092`.
- `ACK_MESSAGE` — Producer acknowledgement mode. Allowed values: `0`, `1`, `all`, `-1`.
- `SCHEMA_REGISTRY_URL` — Schema Registry address, for example `http://schema-registry:8081`.
- `CONSUMER_MODE` — Consumer processing mode. Allowed values: `single` (`s`) or `batch` (`b`).
- `FETCH_MIN_BYTES` — Minimum amount of data the broker should return for a fetch request.
- `FETCH_MAX_MS` — Maximum time the broker waits before returning fetched data.

this values must be written in .env file

## Kafka module docs:

- interrupt module: CTR+C