---
doc: Kafka Incident Escalation Path
topic_id: incident_escalation
version: 1
effective_date: 2025-05-01
superseded: true
status: SUPERSEDED
---

# Kafka Incident Escalation Path (v1)

## Escalation

A SEV-1 Kafka incident escalates to the **Platform Reliability team** via
PagerDuty. The secondary contact is the Infrastructure duty manager.

## Timing

Escalate if not mitigated within 30 minutes.

## Cluster reference

The production cluster is `kafka-prod-east`. Broker `BROKER-EAST-02` listens on
port 9093 for TLS traffic and port 9092 for PLAINTEXT. The metrics endpoint is
`http://BROKER-EAST-02:7071/metrics`. The on-call rota lives in PagerDuty
service `PD-KAFKA-OPS`. Cluster ID is `kfk-east-7731`.
