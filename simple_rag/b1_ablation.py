"""B1 — the ablation: version-aware retrieval OFF vs ON, same frozen golden set.

    RAG_CORPUS=simple_rag/corpus/kafka_ops RAG_COLLECTION=kafka_ops \
        python simple_rag/b1_ablation.py

Only ONE variable changes between the two runs: the `where={"superseded": False}`
condition on the Chroma query. Corpus, golden set, embedding model, chunking and
top-k are all held identical — otherwise it is not an ablation, it is two demos.

Every number here comes from RETRIEVAL only, which is deterministic and free:
no LLM call, no API key, no judge. The B1 headline metric is a property of which
chunks came back, so it costs nothing and cannot be flattered by a good answer
written on top of a bad retrieval.
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import statistics
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

os.environ.setdefault("RAG_CORPUS", "simple_rag/corpus/kafka_ops")
os.environ.setdefault("RAG_COLLECTION", "kafka_ops")
os.environ.setdefault("RAG_TRACE", "0")          # the ablation is the measurement

import rag  # noqa: E402

GOLDEN = HERE / "golden_b1.jsonl"
RESULTS = HERE / "results"
K = 4


def load_cases() -> list[dict]:
    return [json.loads(l) for l in GOLDEN.read_text(encoding="utf-8").splitlines() if l.strip()]


def quiet_retrieve(q: str, version_aware: bool) -> tuple[list[dict], float]:
    """rag.retrieve prints a live table; the ablation wants the numbers only."""
    buf = io.StringIO()
    t0 = time.perf_counter()
    with contextlib.redirect_stdout(buf):
        hits = rag.retrieve(q, k=K, version_aware=version_aware)
    return hits, (time.perf_counter() - t0) * 1000


def cited(hits: list[dict], spec: dict) -> bool:
    """Does any hit match this {doc, version} spec? Compares the human title
    (shared across versions) plus the version integer."""
    for h in hits:
        if h.get("doc_title") == spec["doc"] and int(h.get("version", -1)) == int(spec["version"]):
            return True
    return False


def facts_present(hits: list[dict], facts: list[str]) -> bool:
    blob = " ".join(h["text"] for h in hits)
    return all(f in blob for f in facts)


def run(version_aware: bool, cases: list[dict]) -> dict:
    rows, lat = [], []
    total_chunks = superseded_chunks = 0

    for c in cases:
        hits, ms = quiet_retrieve(c["question"], version_aware)
        lat.append(ms)
        total_chunks += len(hits)
        sup_here = sum(1 for h in hits if h.get("superseded") is True)
        superseded_chunks += sup_here

        exp = c["expect"]
        must_ok = all(cited(hits, s) for s in exp.get("must_cite", []))
        not_ok = not any(cited(hits, s) for s in exp.get("must_not_cite", []))
        fact_ok = facts_present(hits, exp.get("facts", []))

        rows.append({
            "id": c["id"], "type": c["type"], "question": c["question"],
            "hits": [{"doc": h.get("doc_title"), "v": h.get("version"),
                      "superseded": h.get("superseded"),
                      "sim": round(h["similarity"], 3)} for h in hits],
            "superseded_retrieved": sup_here,
            "must_cite_ok": must_ok, "must_not_cite_ok": not_ok,
            "facts_in_context": fact_ok,
            "retrieval_pass": must_ok and not_ok,
            "ms": round(ms, 1),
        })

    def acc(kind: str) -> tuple[int, int]:
        sub = [r for r in rows if r["type"] == kind]
        return sum(1 for r in sub if r["retrieval_pass"]), len(sub)

    ver_p, ver_n = acc("version_current")
    exa_p, exa_n = acc("exact_string")
    mh_p, mh_n = acc("multi_hop")
    fact_p = sum(1 for r in rows if r["type"] == "exact_string" and r["facts_in_context"])

    return {
        "version_aware": version_aware,
        "cases": len(cases),
        "total_chunks_retrieved": total_chunks,
        "superseded_chunks_retrieved": superseded_chunks,
        "superseded_rate_pct": round(100 * superseded_chunks / total_chunks, 1) if total_chunks else 0.0,
        "version_case_retrieval": f"{ver_p}/{ver_n}",
        "version_case_pct": round(100 * ver_p / ver_n, 1) if ver_n else 0.0,
        "exact_string_retrieval": f"{exa_p}/{exa_n}",
        "exact_string_facts_in_context": f"{fact_p}/{exa_n}",
        "exact_string_pct": round(100 * fact_p / exa_n, 1) if exa_n else 0.0,
        "multi_hop_retrieval": f"{mh_p}/{mh_n}",
        "median_latency_ms": round(statistics.median(lat), 1),
        "mean_latency_ms": round(statistics.mean(lat), 1),
        "rows": rows,
    }


def main() -> None:
    cases = load_cases()
    RESULTS.mkdir(exist_ok=True)
    print(f"B1 ablation · {len(cases)} frozen cases · k={K} · collection={rag.COLLECTION}")
    print(f"corpus: {rag.CORPUS}\n")

    off = run(False, cases)
    print(f"  filter OFF : {off['superseded_chunks_retrieved']:>3}/{off['total_chunks_retrieved']} "
          f"superseded = {off['superseded_rate_pct']}%")
    on = run(True, cases)
    print(f"  filter ON  : {on['superseded_chunks_retrieved']:>3}/{on['total_chunks_retrieved']} "
          f"superseded = {on['superseded_rate_pct']}%\n")

    (RESULTS / "baseline_filter_off.json").write_text(json.dumps(off, indent=2), encoding="utf-8")
    (RESULTS / "version_filter_on.json").write_text(json.dumps(on, indent=2), encoding="utf-8")

    w = 34
    rows = [
        ("Superseded retrieved chunks", f"{off['superseded_rate_pct']}%", f"{on['superseded_rate_pct']}%", "0%"),
        ("Version-case retrieval", f"{off['version_case_pct']}%", f"{on['version_case_pct']}%", "improve"),
        ("Exact-string facts in context", f"{off['exact_string_pct']}%", f"{on['exact_string_pct']}%", "stay flat"),
        ("Multi-hop retrieval", off["multi_hop_retrieval"], on["multi_hop_retrieval"], "improve"),
        ("Median retrieval latency", f"{off['median_latency_ms']} ms", f"{on['median_latency_ms']} ms", "small change"),
    ]
    line = "  " + "-" * (w + 34)
    print(f"  {'Metric':<{w}}{'OFF':>10}{'ON':>10}{'Target':>14}")
    print(line)
    for name, a, b, t in rows:
        print(f"  {name:<{w}}{a:>10}{b:>10}{t:>14}")
    print(line)

    md = [
        "# B1 — Ablation Summary\n",
        f"Corpus: `{rag.CORPUS}` · collection `{rag.COLLECTION}` · k={K} · "
        f"{len(cases)} frozen golden cases\n",
        "Only one variable changes: `where={\"superseded\": False}` on the Chroma query.\n",
        "| Metric | Filter OFF | Filter ON | Target |",
        "|---|---|---|---|",
    ]
    for name, a, b, t in rows:
        md.append(f"| {name} | {a} | {b} | {t} |")
    md.append("")
    md.append(f"**Headline:** superseded retrieval went from **{off['superseded_rate_pct']}%** "
              f"to **{on['superseded_rate_pct']}%**, while exact-string facts stayed at "
              f"**{on['exact_string_pct']}%** (was {off['exact_string_pct']}%).")
    (RESULTS / "ablation_summary.md").write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"\n  wrote {RESULTS/'baseline_filter_off.json'}")
    print(f"  wrote {RESULTS/'version_filter_on.json'}")
    print(f"  wrote {RESULTS/'ablation_summary.md'}")


if __name__ == "__main__":
    main()
