---
doc: Kafka Broker Restart Procedure
topic_id: broker_restart
version: 1
effective_date: 2025-03-01
superseded: true
status: SUPERSEDED
---

# Kafka Broker Restart Procedure (v1)

## Restart sequence

1. Stop the broker process.
2. Drain remaining partition leadership.
3. Apply the patch.
4. Start the broker and wait for ISR to catch up.

Expected total downtime is 12 minutes per broker.

## Cluster reference

The production cluster is `kafka-prod-east`. Broker `BROKER-EAST-02` listens on
port 9093 for TLS traffic and port 9092 for PLAINTEXT. The metrics endpoint is
`http://BROKER-EAST-02:7071/metrics`. The on-call rota lives in PagerDuty
service `PD-KAFKA-OPS`. Cluster ID is `kfk-east-7731`.
