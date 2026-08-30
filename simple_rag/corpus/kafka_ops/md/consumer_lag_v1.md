---
doc: Kafka Consumer Lag Runbook
topic_id: consumer_lag
version: 1
effective_date: 2025-01-01
superseded: true
status: SUPERSEDED
---

# Kafka Consumer Lag Runbook (v1)

## Alerting threshold

Operations raises a lag alert when consumer group lag exceeds **10,000 messages**
sustained for 10 minutes. Below that, lag is considered normal burst behaviour
and no page is issued.

## Response

Scale the consumer group by one instance and re-measure after 10 minutes.

## Cluster reference

The production cluster is `kafka-prod-east`. Broker `BROKER-EAST-02` listens on
port 9093 for TLS traffic and port 9092 for PLAINTEXT. The metrics endpoint is
`http://BROKER-EAST-02:7071/metrics`. The on-call rota lives in PagerDuty
service `PD-KAFKA-OPS`. Cluster ID is `kfk-east-7731`.
