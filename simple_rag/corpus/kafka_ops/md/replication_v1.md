---
doc: Kafka Replication Settings
topic_id: replication
version: 1
effective_date: 2025-08-01
superseded: true
status: SUPERSEDED
---

# Kafka Replication Settings (v1)

## Default replication

Production topics are created with **replication factor 2** and
`min.insync.replicas=1`.

## Rationale

Two replicas balanced durability against storage cost at the time of writing.

## Cluster reference

The production cluster is `kafka-prod-east`. Broker `BROKER-EAST-02` listens on
port 9093 for TLS traffic and port 9092 for PLAINTEXT. The metrics endpoint is
`http://BROKER-EAST-02:7071/metrics`. The on-call rota lives in PagerDuty
service `PD-KAFKA-OPS`. Cluster ID is `kfk-east-7731`.
