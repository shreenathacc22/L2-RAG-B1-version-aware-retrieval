---
doc: Kafka Consumer Lag Runbook
topic_id: consumer_lag
version: 2
effective_date: 2026-01-01
superseded: false
status: CURRENT
---

# Kafka Consumer Lag Runbook (v2)

## Alerting threshold

Operations raises a lag alert when consumer group lag exceeds **5,000 messages**
sustained for 5 minutes. The previous 10,000-message threshold was found to
detect incidents too late and is no longer in force.

## Response

Scale the consumer group by one instance and re-measure after 5 minutes. If lag
is still climbing, escalate before adding a second instance.

## Cluster reference

The production cluster is `kafka-prod-east`. Broker `BROKER-EAST-02` listens on
port 9093 for TLS traffic and port 9092 for PLAINTEXT. The metrics endpoint is
`http://BROKER-EAST-02:7071/metrics`. The on-call rota lives in PagerDuty
service `PD-KAFKA-OPS`. Cluster ID is `kfk-east-7731`.
