---
doc: Kafka Incident Escalation Path
topic_id: incident_escalation
version: 2
effective_date: 2026-05-01
superseded: false
status: CURRENT
---

# Kafka Incident Escalation Path (v2)

## Escalation

A SEV-1 Kafka incident escalates to the **Streaming Platform team** via
PagerDuty. Platform Reliability no longer owns Kafka and must not be paged for
streaming incidents. The secondary contact is the Infrastructure duty manager.

## Timing

Escalate if not mitigated within 15 minutes.

## Cluster reference

The production cluster is `kafka-prod-east`. Broker `BROKER-EAST-02` listens on
port 9093 for TLS traffic and port 9092 for PLAINTEXT. The metrics endpoint is
`http://BROKER-EAST-02:7071/metrics`. The on-call rota lives in PagerDuty
service `PD-KAFKA-OPS`. Cluster ID is `kfk-east-7731`.
