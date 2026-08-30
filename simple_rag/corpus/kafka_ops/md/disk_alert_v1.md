---
doc: Kafka Disk Utilisation Alerting
topic_id: disk_alert
version: 1
effective_date: 2025-07-01
superseded: true
status: SUPERSEDED
---

# Kafka Disk Utilisation Alerting (v1)

## Threshold

Page when broker log-directory utilisation exceeds **85%**. Warn at 70%.

## Action

Expand the volume or reduce retention on the largest topic.

## Cluster reference

The production cluster is `kafka-prod-east`. Broker `BROKER-EAST-02` listens on
port 9093 for TLS traffic and port 9092 for PLAINTEXT. The metrics endpoint is
`http://BROKER-EAST-02:7071/metrics`. The on-call rota lives in PagerDuty
service `PD-KAFKA-OPS`. Cluster ID is `kfk-east-7731`.
