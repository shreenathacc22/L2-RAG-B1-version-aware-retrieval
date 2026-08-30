---
doc: Kafka Replication Settings
topic_id: replication
version: 2
effective_date: 2026-08-01
superseded: false
status: CURRENT
---

# Kafka Replication Settings (v2)

## Default replication

Production topics are created with **replication factor 3** and
`min.insync.replicas=2`. Replication factor 2 is no longer acceptable for
production and any topic still at 2 must be migrated.

## Rationale

Three replicas survive a single broker loss without dropping below minimum ISR.

## Cluster reference

The production cluster is `kafka-prod-east`. Broker `BROKER-EAST-02` listens on
port 9093 for TLS traffic and port 9092 for PLAINTEXT. The metrics endpoint is
`http://BROKER-EAST-02:7071/metrics`. The on-call rota lives in PagerDuty
service `PD-KAFKA-OPS`. Cluster ID is `kfk-east-7731`.
