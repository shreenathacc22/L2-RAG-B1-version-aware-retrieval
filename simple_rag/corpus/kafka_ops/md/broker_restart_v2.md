---
doc: Kafka Broker Restart Procedure
topic_id: broker_restart
version: 2
effective_date: 2026-03-01
superseded: false
status: CURRENT
---

# Kafka Broker Restart Procedure (v2)

## Restart sequence

1. Drain partition leadership away from the broker first.
2. Verify no partitions list this broker as leader.
3. Stop the broker process.
4. Apply the patch.
5. Start the broker and wait for ISR to catch up.

Draining before stopping removes the leadership-election stall that made the old
sequence slow. Expected total downtime is 4 minutes per broker.

## Cluster reference

The production cluster is `kafka-prod-east`. Broker `BROKER-EAST-02` listens on
port 9093 for TLS traffic and port 9092 for PLAINTEXT. The metrics endpoint is
`http://BROKER-EAST-02:7071/metrics`. The on-call rota lives in PagerDuty
service `PD-KAFKA-OPS`. Cluster ID is `kfk-east-7731`.
