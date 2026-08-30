---
doc: Kafka TLS Certificate Rotation
topic_id: tls_procedure
version: 1
effective_date: 2025-04-01
superseded: true
status: SUPERSEDED
---

# Kafka TLS Certificate Rotation (v1)

## Certificate source

Request broker certificates from `certs.internal.example.com` using the
`kafka-broker` template. Certificates are valid for 365 days.

## Rotation

Rotate all broker certs together during a maintenance window.

## Cluster reference

The production cluster is `kafka-prod-east`. Broker `BROKER-EAST-02` listens on
port 9093 for TLS traffic and port 9092 for PLAINTEXT. The metrics endpoint is
`http://BROKER-EAST-02:7071/metrics`. The on-call rota lives in PagerDuty
service `PD-KAFKA-OPS`. Cluster ID is `kfk-east-7731`.
