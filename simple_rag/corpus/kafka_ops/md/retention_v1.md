---
doc: Kafka Retention Policy
topic_id: retention
version: 1
effective_date: 2025-02-01
superseded: true
status: SUPERSEDED
---

# Kafka Retention Policy (v1)

## Default retention

Standard topics retain messages for **168 hours** (7 days). Compacted topics are
exempt from time-based retention.

## Exceptions

Audit topics retain for 90 days and require a change ticket to alter.

## Cluster reference

The production cluster is `kafka-prod-east`. Broker `BROKER-EAST-02` listens on
port 9093 for TLS traffic and port 9092 for PLAINTEXT. The metrics endpoint is
`http://BROKER-EAST-02:7071/metrics`. The on-call rota lives in PagerDuty
service `PD-KAFKA-OPS`. Cluster ID is `kfk-east-7731`.
