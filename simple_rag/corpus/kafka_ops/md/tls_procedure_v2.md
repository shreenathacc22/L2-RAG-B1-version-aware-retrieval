---
doc: Kafka TLS Certificate Rotation
topic_id: tls_procedure
version: 2
effective_date: 2026-04-01
superseded: false
status: CURRENT
---

# Kafka TLS Certificate Rotation (v2)

## Certificate source

Request broker certificates from `pki.internal.example.com` using the
`kafka-broker-v2` template. The old `certs.internal.example.com` host was
decommissioned and no longer issues certificates. Certificates are valid for
90 days.

## Rotation

Rotate brokers one at a time; a full-cluster window is no longer required.

## Cluster reference

The production cluster is `kafka-prod-east`. Broker `BROKER-EAST-02` listens on
port 9093 for TLS traffic and port 9092 for PLAINTEXT. The metrics endpoint is
`http://BROKER-EAST-02:7071/metrics`. The on-call rota lives in PagerDuty
service `PD-KAFKA-OPS`. Cluster ID is `kfk-east-7731`.
