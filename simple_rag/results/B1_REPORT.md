# B1 — Version-Aware Retrieval

**Modern AI Pro · Level 2 · Track B · RAG Design**
Team: Shreenath and Selvan · Repo: `mai-practitioner-labs`

**Objective:** prevent superseded documents from winning retrieval on semantic similarity alone, by adding version metadata and filtering at query time.

**Success condition:** superseded retrieved chunks drop to 0% while `exact_string` performance stays flat, measured on the *same frozen golden set* before and after.

---

## Result

| Metric | Filter OFF | Filter ON | Target | Met |
|---|---|---|---|---|
| **Superseded retrieved chunks** | **53.1%** | **0.0%** | 0% | ✅ |
| Version-case retrieval accuracy | 0.0% | 100.0% | improve | ✅ |
| Exact-string facts in context | 100.0% | 100.0% | stay flat | ✅ |
| Multi-hop retrieval | 0/2 | 2/2 | improve | ✅ |
| Median retrieval latency | 38.7 ms | 37.2 ms | small change | ✅ |

> Adding `version` / `effective_date` / `superseded` metadata and excluding superseded chunks at query time reduced stale retrieval from **53.1% to 0%**. Exact-string retrieval remained **100%**, showing the version-aware stage fixed stale-document selection without degrading normal retrieval.

Latency did not increase. Chroma applies the `where` clause during search, so the filter removes candidates rather than adding a pass.

---

## What was built

### The corpus — 8 topics × 2 versions = 16 files

`simple_rag/corpus/kafka_ops/md/` — Kafka operations runbooks, our own domain (not Meridian, not Aventro).

| Topic | v1 (superseded) | v2 (current) | Conflicting fact |
|---|---|---|---|
| Consumer lag | 2025-01-01 | 2026-01-01 | 10,000 → **5,000 messages** |
| Retention | 2025-02-01 | 2026-02-01 | 168h → **72h** |
| Broker restart | 2025-03-01 | 2026-03-01 | stop-then-drain → **drain-then-stop** |
| TLS rotation | 2025-04-01 | 2026-04-01 | `certs.internal` → **`pki.internal`** |
| Incident escalation | 2025-05-01 | 2026-05-01 | Platform Reliability → **Streaming Platform** |
| Topic naming | 2025-06-01 | 2026-06-01 | `evt.` → **`event.`** |
| Disk alert | 2025-07-01 | 2026-07-01 | 85% → **75%** |
| Replication | 2025-08-01 | 2026-08-01 | factor 2 → **factor 3** |

Every file also carries an **identical** infrastructure block — cluster name, ports, PagerDuty service, cluster ID. Those are the version-neutral facts the `exact_string` control cases probe, so a flat score there is meaningful rather than accidental.

Front-matter:

```yaml
---
doc: Kafka Consumer Lag Runbook
topic_id: consumer_lag
version: 2
effective_date: 2026-01-01
superseded: false
status: CURRENT
---
```

### The golden set — 16 cases, frozen first

`simple_rag/golden_b1.jsonl`, committed at **`04c830c`** *before* any retrieval change. The version-aware code landed at **`149bb96`**. The git history is the evidence of ordering.

| Type | Count | Purpose |
|---|---|---|
| `version_current` | 8 | old and current conflict — only the current fact passes; `must_not_cite` the v1 |
| `exact_string` | 6 | facts identical in both versions — the flat-baseline control |
| `multi_hop` | 2 | two topics, each with a superseded twin |

### The code change — surgical, in `simple_rag/rag.py`

**1 · Parse front-matter** (`read_md` → `_split_front_matter`). Corpora without a block are unaffected — Aventro still ingests unchanged.

**2 · Carry the metadata** doc → chunk → Chroma, named once so the three stages cannot disagree:

```python
B1_FIELDS = ("doc_title", "topic_id", "version", "effective_date", "superseded")
```

**3 · The one new retrieval stage:**

```python
def retrieve(query, k=4, version_aware=False):
    ...
    where = {"superseded": False} if version_aware else None
    res = collection().query(query_embeddings=[qv], n_results=k, where=where,
                             include=["documents", "metadatas", "distances"])
```

**4 · Env overrides** (`RAG_CORPUS`, `RAG_COLLECTION`) so the OFF and ON runs cannot drift onto different corpora by accident.

One design note worth stating: the front-matter `doc:` title becomes `doc_title`, **not** the dedupe key `stem`. Two versions sharing a stem would have been silently de-duplicated at ingest — which would have deleted the experiment before it ran.

---

## Evidence — why this is a filter, not a re-rank

`b1-001` — *"At what consumer lag should operations raise an alert?"*

```
OFF: Consumer Lag v2  0.522 | Consumer Lag v1* 0.498 | Disk v2 0.303 | Disk v1* 0.284
ON : Consumer Lag v2  0.522 | Disk v2 0.303 | Retention v2 0.234 | Escalation v2 0.233
```

The superseded chunk sat **0.024 behind** the current one. Similarity alone cannot separate near-identical versions — which is the entire premise of B1.

**The surviving chunk keeps its exact score (0.522 both ways.)** Nothing was re-ranked or re-weighted; the ineligible candidates were removed before ranking.

`b1-009` (control) — *"What port does BROKER-EAST-02 use for TLS traffic?"* — top hit `0.561` in both runs. The filter did not disturb version-neutral retrieval.

---

## Failures and honest limitations

**1 · The 53.1% baseline is inflated by corpus design.** The corpus is exactly 50% superseded and every topic has a twin, with `k=4` drawing from only 16 chunks. A production corpus with a smaller superseded fraction would show a much lower baseline. **The direction is real; the magnitude is a property of this fixture.**

**2 · Version-case retrieval scores 0% at baseline, which looks too clean.** It is correct but strict: a case fails if the v1 chunk appears *at all*, and with near-identical twins the v1 almost always makes the top-4. A softer metric (did the v2 rank above the v1?) would show baseline partial credit.

**3 · Every document fits in one chunk.** These runbooks are short, so chunking never split a document and `must_cite` is effectively doc-level. The `golden/SCHEMA.md` open item — chunk-level assertion — is not exercised here.

**4 · Retrieval only; no answer-quality measurement.** The headline metric is deliberately retrieval-only, which makes it free and deterministic. We have **not** yet shown that better retrieval produced better *answers* — that needs the LLM judge and an API key.

**5 · Filter semantics are exclusion, not recency.** `where={"superseded": False}` trusts the flag. It does not reason about `effective_date`, so a corpus with three live versions, or one where the flag is stale, would need date comparison instead.

---

## Cost and latency

| | OFF | ON |
|---|---|---|
| Median retrieval | 38.7 ms | 37.2 ms |
| Mean retrieval | — | — *(see JSON)* |
| LLM calls | **0** | **0** |
| API cost | **$0** | **$0** |

Embeddings run locally (`all-MiniLM-L6-v2`, 384-dim, Apache-2.0). Because the B1 metric is a property of *which chunks came back*, the whole experiment costs nothing and cannot be flattered by a well-written answer sitting on top of bad retrieval.

---

## Reproduce it

```bash
cd mai-practitioner-labs
export RAG_CORPUS=simple_rag/corpus/kafka_ops
export RAG_COLLECTION=kafka_ops
export PYTHONIOENCODING=utf-8

python simple_rag/rag.py ingest --formats md     # 16 chunks, metadata attached
python simple_rag/b1_ablation.py                 # OFF vs ON, writes results/
```

Outputs in `simple_rag/results/`: `baseline_filter_off.json`, `version_filter_on.json`, `ablation_summary.md`.

---

## Environment note

`torch` cannot install into the lab `.venv` on this machine: the venv path is 107 characters, `torch` nests ~160 deeper, and `LongPathsEnabled = 0` — producing `WinError 206`. Rather than requiring an admin registry change, the dependencies were installed into a **short-path virtualenv at `C:\wv`**, which sidesteps the 260-character limit entirely. Everything above ran from there.

---

## Submission checklist

| Requirement | Status |
|---|---|
| Start from `simple_rag/` | ✅ extended, not rebuilt |
| Own corpus | ✅ Kafka ops, 16 files |
| Golden set frozen first | ✅ `04c830c` before `149bb96` |
| At least 15 cases | ✅ 16 |
| Metadata added | ✅ version, effective_date, superseded |
| Filter at query time | ✅ `where={"superseded": False}` |
| Ablation run | ✅ one variable, same frozen set |
| B1 metric shown | ✅ 53.1% → 0.0% |
| Exact-string protected | ✅ 100% → 100% |
| Evidence produced | ✅ two JSONs + summary + this report |
| Failures documented | ✅ five limitations above |

**Outstanding for the team:** Step 1 of the guide — the introduction call with Selvan and agreement on the shared branch workflow — has not happened. Nothing here is pushed; both commits are local.
