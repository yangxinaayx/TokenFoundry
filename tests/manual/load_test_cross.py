#!/usr/bin/env python3
"""Token Foundry — dual-methodology cross-validation (no need to run load_test_ramp.py).

Answers two questions in one script:
  1. What is MY safe concurrency?  (--discover)
  2. Does batch vs pipeline agree on TPM/RPS?  (--compare, default)

Two **different load generators** (same metrics, same formulas as CAPACITY.zh.md):

  +------------------+---------------------------+---------------------------+
  |                  | BATCH (doc / ramp logic)  | PIPELINE (new logic)      |
  +------------------+---------------------------+---------------------------+
  | Scheduling       | fire N requests, wait     | keep N in-flight for T s  |
  | Implementation   | ThreadPoolExecutor.map    | worker loop + timer       |
  | Wall clock       | batch drain time          | fixed measurement window  |
  | Same as ramp.py? | YES (logic, not the file) | NO — independent          |
  +------------------+---------------------------+---------------------------+

Cross-test PASS when both methods at the same concurrency report:
  * 0×429 and 0×503 (or negligible)
  * TPM within --tpm-tolerance (default 15%)

Requirements:
    TF_GATEWAY_URL + TF_VIRTUAL_KEY in .env (UNLIMITED virtual key)

Usage:
    # Recommended: find concurrency, then cross-compare both methods
    python tests/manual/load_test_cross.py --discover
    python tests/manual/load_test_cross.py --compare -c 16

    # One command: discover sweet spot, cooldown, then batch vs pipeline there
    python tests/manual/load_test_cross.py --discover --then-compare
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from load_test_common import (
    RESULT_HDR,
    format_result_row,
    gateway_from_env,
    load_dotenv_if_present,
    make_body,
    require,
    run_level,
    run_pipeline,
)

DEFAULT_CONCURRENCY = 24
DEFAULT_COOLDOWN = 90
DEFAULT_MAX_TOKENS = 1200
DEFAULT_TIMEOUT = 180
DEFAULT_TPM_TOLERANCE = 0.15
DEFAULT_DISCOVER_LEVELS = "8,16,24,32,40"
DEFAULT_PHASE_DURATION = 90
DEFAULT_PIPELINE_DURATION = 90
DISCOVER_STOP_ERROR_RATE = 0.5
DOC_BASELINE_TPM = 41_602


def parse_int_list(raw: str, name: str) -> list[int]:
    parts = [int(x.strip()) for x in raw.split(",") if x.strip()]
    if not parts or any(p < 1 for p in parts):
        sys.exit(f"ERROR: {name} must be positive integers, comma-separated")
    return parts


def is_clean(row: dict) -> bool:
    return row["c429"] == 0 and row["c503"] == 0


def error_rate(row: dict) -> float:
    return 1 - (row["ok"] / row["n"] if row["n"] else 0)


def rel_diff(a: float, b: float) -> float:
    denom = max(abs(a), abs(b), 1e-9)
    return abs(a - b) / denom


def tag_row(row: dict, method: str) -> dict:
    out = dict(row)
    out["method"] = method
    return out


def run_batch(
    url: str, headers: dict, body: dict, conc: int, per_level: int, timeout: int
) -> dict:
    print(f"\n--- BATCH (ramp logic): conc={conc}  requests={per_level} ---", flush=True)
    row = run_level(url, headers, body, conc, per_level, timeout)
    print(format_result_row(row))
    print(f"     wall={row['wall']:.1f}s  tokens(ok)={row['tokens']}")
    if row["sample_err"]:
        print(f"     err sample: {row['sample_err'][:110]}")
    return tag_row(row, "batch")


def run_pipe(
    url: str, headers: dict, body: dict, conc: int, duration: int, timeout: int
) -> dict:
    print(f"\n--- PIPELINE (new logic): conc={conc}  duration={duration}s ---", flush=True)
    row = run_pipeline(url, headers, body, conc, float(duration), timeout)
    print(format_result_row(row))
    print(f"     n={row['n']}  wall={row['wall']:.0f}s  tokens(ok)={row['tokens']}")
    if row["sample_err"]:
        print(f"     err sample: {row['sample_err'][:110]}")
    return tag_row(row, "pipeline")


def print_method_cross(batch: dict, pipe: dict, tpm_tol: float) -> None:
    print("\n" + "=" * 68)
    print("CROSS-TEST: BATCH (doc/ramp logic)  vs  PIPELINE (new logic)")
    print("=" * 68)
    print(f"{'method':<10} {RESULT_HDR}")
    print("-" * 68)
    for r in (batch, pipe):
        print(f"{r['method']:<10} {format_result_row(r)}")

    print(f"\n{'':10} conc={batch['conc']}  max_tokens={DEFAULT_MAX_TOKENS}")
    print(f"{'batch':<10} per_level={batch['n']}  wall={batch['wall']:.1f}s")
    print(f"{'pipeline':<10} duration={int(pipe['wall'])}s  completed={pipe['n']}")

    if is_clean(batch) and is_clean(pipe):
        tpm_d = rel_diff(batch["tpm"], pipe["tpm"])
        rps_d = rel_diff(batch["rps"], pipe["rps"])
        print(f"\nBoth methods CLEAN (0×429, 0×503)")
        print(f"  TPM: batch={batch['tpm']:.0f}  pipeline={pipe['tpm']:.0f}  "
              f"delta={tpm_d:.1%}  (tolerance {tpm_tol:.0%})")
        print(f"  RPS: batch={batch['rps']:.2f}  pipeline={pipe['rps']:.2f}  "
              f"delta={rps_d:.1%}")
        if tpm_d <= tpm_tol:
            print("\nVERDICT: PASS — cross-test agrees; TPM/RPS trustworthy.")
        else:
            print("\nVERDICT: MARGINAL — methods differ; prefer BATCH TPM (matches doc) "
                  "or re-run after cooldown.")
        doc_delta = (max(batch["tpm"], pipe["tpm"]) - DOC_BASELINE_TPM) / DOC_BASELINE_TPM
        print(f"\nBest TPM vs CAPACITY.zh.md ({DOC_BASELINE_TPM:,}): {doc_delta:+.1%}")
    else:
        print("\nVERDICT: FAIL — at least one method hit 429/503.")
        for r in (batch, pipe):
            if not is_clean(r):
                print(f"  {r['method']}: {r['c429']}×429  {r['c503']}×503")
        print("  → Lower --concurrency or wait 90s and retry.")
        if not is_clean(batch) and is_clean(pipe):
            print(f"  Pipeline-only TPM={pipe['tpm']:.0f} (approximate, batch was dirty).")
        elif is_clean(batch) and not is_clean(pipe):
            print(f"  Batch-only TPM={batch['tpm']:.0f} (prefer this; pipeline hit breaker).")
    print("=" * 68)


def run_discover(
    args: argparse.Namespace, url: str, headers: dict, body: dict
) -> tuple[list[dict], int | None]:
    levels = parse_int_list(args.levels, "--levels")
    rows: list[dict] = []
    print("=== CONCURRENCY DISCOVERY (pipeline only) ===")
    print(f"levels={levels}  {args.phase_duration}s each  cooldown={args.cooldown}s\n")

    sweet_conc: int | None = None
    try:
        for i, conc in enumerate(levels):
            if i > 0 and args.cooldown > 0:
                print(f"... cooldown {args.cooldown}s ...", flush=True)
                time.sleep(args.cooldown)
            row = run_pipe(url, headers, body, conc, args.phase_duration, args.timeout)
            rows.append(row)
            if is_clean(row):
                sweet_conc = conc
            if error_rate(row) > DISCOVER_STOP_ERROR_RATE:
                print(f">>> error rate {error_rate(row):.0%} — stop discovery")
                break
    except KeyboardInterrupt:
        print(">>> interrupted")

    print("\n--- Discovery summary ---")
    print(RESULT_HDR)
    for r in rows:
        mark = "clean" if is_clean(r) else "DIRTY"
        print(f"{format_result_row(r)}  [{mark}]")
    if sweet_conc is not None:
        best = max((r for r in rows if is_clean(r)), key=lambda r: r["conc"])
        print(f"\nSWEET SPOT (max clean concurrency): {best['conc']}  "
              f"TPM={best['tpm']:.0f}  RPS={best['rps']:.2f}")
    else:
        print("\nNo clean level found — try --levels 4,8,12 or wait longer between steps.")
    return rows, sweet_conc


def run_compare(args: argparse.Namespace, url: str, headers: dict, body: dict, conc: int) -> int:
    per_level = args.per_level if args.per_level > 0 else conc * 3
    print(f"=== METHOD CROSS-TEST @ concurrency={conc} ===")
    print(f"target: {url}  model: {body['model']}  max_tokens={args.max_tokens}\n")

    batch = run_batch(url, headers, body, conc, per_level, args.timeout)
    if args.cooldown > 0:
        print(f"\n... cooldown {args.cooldown}s (reset breaker) ...", flush=True)
        time.sleep(args.cooldown)
    pipe = run_pipe(url, headers, body, conc, args.pipeline_duration, args.timeout)
    print_method_cross(batch, pipe, args.tpm_tolerance)
    return 0 if is_clean(batch) and is_clean(pipe) else 1


def main() -> int:
    load_dotenv_if_present()

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--discover", action="store_true",
                      help="step 1: sweep concurrency (pipeline) to find sweet spot")
    mode.add_argument("--compare", action="store_true",
                      help="step 2: batch vs pipeline at one concurrency (default if neither flag)")
    ap.add_argument("--then-compare", action="store_true",
                    help="with --discover: auto-run --compare at discovered sweet spot")
    ap.add_argument("--levels", default=DEFAULT_DISCOVER_LEVELS)
    ap.add_argument("--phase-duration", type=int, default=DEFAULT_PHASE_DURATION,
                    help="seconds per level in --discover")
    ap.add_argument("-c", "--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                    help="concurrency for --compare")
    ap.add_argument("--per-level", type=int, default=0,
                    help="batch request count (default conc×3, doc uses 72 @ conc=24)")
    ap.add_argument("--pipeline-duration", type=int, default=DEFAULT_PIPELINE_DURATION,
                    help="pipeline seconds in --compare")
    ap.add_argument("--cooldown", type=int, default=DEFAULT_COOLDOWN)
    ap.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("--tpm-tolerance", type=float, default=DEFAULT_TPM_TOLERANCE)
    args = ap.parse_args()

    require("TF_GATEWAY_URL")
    require("TF_VIRTUAL_KEY")

    url, headers, model = gateway_from_env()
    body = make_body(model, args.max_tokens)

    # Default action: --compare (if no mode flag)
    if not args.discover and not args.compare:
        args.compare = True

    if args.discover:
        _, sweet = run_discover(args, url, headers, body)
        if args.then_compare and sweet is not None:
            if args.cooldown > 0:
                print(f"\n... cooldown {args.cooldown}s before cross-test ...", flush=True)
                time.sleep(args.cooldown)
            return run_compare(args, url, headers, body, sweet)
        return 0 if sweet is not None else 1

    return run_compare(args, url, headers, body, args.concurrency)


if __name__ == "__main__":
    sys.exit(main())
