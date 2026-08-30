# B1 — Ablation Summary

Corpus: `C:\Users\shree\OneDrive\Desktop\vvip_AI-Architect_L2-Aug-2026\mai-practitioner-labs\simple_rag\corpus\kafka_ops` · collection `kafka_ops` · k=4 · 16 frozen golden cases

Only one variable changes: `where={"superseded": False}` on the Chroma query.

| Metric | Filter OFF | Filter ON | Target |
|---|---|---|---|
| Superseded retrieved chunks | 53.1% | 0.0% | 0% |
| Version-case retrieval | 0.0% | 100.0% | improve |
| Exact-string facts in context | 100.0% | 100.0% | stay flat |
| Multi-hop retrieval | 0/2 | 2/2 | improve |
| Median retrieval latency | 38.7 ms | 37.2 ms | small change |

**Headline:** superseded retrieval went from **53.1%** to **0.0%**, while exact-string facts stayed at **100.0%** (was 100.0%).
