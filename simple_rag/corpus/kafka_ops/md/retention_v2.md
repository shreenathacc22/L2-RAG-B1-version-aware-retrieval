---
doc: Kafka Retention Policy
topic_id: retention
version: 2
effective_date: 2026-02-01
superseded: false
status: CURRENT
---

# Kafka Retention Policy (v2)

## Default retention

Standard topics retain messages for **72 hours** (3 days). The former 168-hour
default was reduced after the storage review; any runbook still quoting 7 days
is superseded.

## Exceptions

Audit topics retain for 90 days and require a change ticket to alter.

## Cluster reference

The production cluster is `kafka-prod-east`. Broker `BROKER-EAST-02` listens on
port 9093 for TLS traffic and port 9092 for PLAINTEXT. The metrics endpoint is
`http://BROKER-EAST-02:7071/metrics`. The on-call rota lives in PagerDuty
service `PD-KAFKA-OPS`. Cluster ID is `kfk-east-7731`.
