---
doc: Kafka Topic Naming Standard
topic_id: topic_naming
version: 2
effective_date: 2026-06-01
superseded: false
status: CURRENT
---

# Kafka Topic Naming Standard (v2)

## Prefix

Event topics use the prefix `event.` followed by domain and entity, for example
`event.orders.created`. The abbreviated `evt.` prefix is retired; new topics
using it will be rejected by the topic-admission check.

## Casing

Lowercase with dots as separators. Hyphens are not permitted.

## Cluster reference

The production cluster is `kafka-prod-east`. Broker `BROKER-EAST-02` listens on
port 9093 for TLS traffic and port 9092 for PLAINTEXT. The metrics endpoint is
`http://BROKER-EAST-02:7071/metrics`. The on-call rota lives in PagerDuty
service `PD-KAFKA-OPS`. Cluster ID is `kfk-east-7731`.
