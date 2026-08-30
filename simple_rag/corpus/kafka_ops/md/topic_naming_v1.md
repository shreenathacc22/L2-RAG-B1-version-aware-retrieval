---
doc: Kafka Topic Naming Standard
topic_id: topic_naming
version: 1
effective_date: 2025-06-01
superseded: true
status: SUPERSEDED
---

# Kafka Topic Naming Standard (v1)

## Prefix

Event topics use the prefix `evt.` followed by domain and entity, for example
`evt.orders.created`.

## Casing

Lowercase with dots as separators. Hyphens are not permitted.

## Cluster reference

The production cluster is `kafka-prod-east`. Broker `BROKER-EAST-02` listens on
port 9093 for TLS traffic and port 9092 for PLAINTEXT. The metrics endpoint is
`http://BROKER-EAST-02:7071/metrics`. The on-call rota lives in PagerDuty
service `PD-KAFKA-OPS`. Cluster ID is `kfk-east-7731`.
