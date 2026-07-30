#!/usr/bin/env python3
"""Token Foundry — sustained load test (batch-repeat mode).

Repeats the same ``run_level`` batch as ``load_test_ramp.py`` at fixed concurrency.
For an **independent** TPM cross-check (different scheduling algorithm), use
``load_test_verify.py`` instead — it uses continuous pipeline mode.

    conc   ok  429  503  err     RPS       TPM   p50 s   p95 s

Use this to validate that a sweet-spot concurrency (e.g. 24 for one GitHub account)
stays stable over time, or to reproduce the "long run" 429 accumulation called out
in CAPACITY.zh.md §2.5.

**Presets** mirror the documented ramp scenarios (§5) but run as repeated rounds
at one concurrency instead of a level sweep:

    probe   — 16 concurrent, 12 req/round  (quick sanity check)
    tpm     — 24 concurrent, 72 req/round, max_tokens=1200  (TPM ceiling run)
    ceiling — 48 concurrent, 48 req/round  (find overload; use with care)

Requirements (same as ramp):
    * UNLIMITED virtual key (no tokens_per_minute on the key)
    * TF_GATEWAY_URL + TF_VIRTUAL_KEY in env or repo-root .env
    * per-level >= concurrency × 2 (×3 for slow models) — enforced with a warning

Usage:
    # Default: tpm preset — 24 concurrent, 72 req/round, 5 minutes
    python tests/manual/load_test_sustain.py

    # Documented ceiling probe (48 concurrent rounds; cooldown >= 70s recommended)
    python tests/manual/load_test_sustain.py --preset ceiling --cooldown 70

    # Custom fixed concurrency soak
    python tests/manual/load_test_sustain.py -c 24 --per-level 72 --max-tokens 1200 \\
        --timeout 180 --duration 600 --cooldown 20
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
    print_peak_summary,
    require,
    run_level,
    error_rate,
)

# Defaults aligned with CAPACITY.zh.md §5 "找 TPM 上限" recommendation.
PRESET = "tpm"
CONCURRENCY = 24
PER_LEVEL = 72
MAX_TOKENS = 1200
TIMEOUT = 180
DURATION = 300
COOLDOWN = 20
STOP_ERROR_RATE = 0.5

PRESETS = {
    "probe": {"concurrency": 16, "per_level": 12, "max_tokens": 60, "timeout": 90},
    "tpm": {"concurrency": 24, "per_level": 72, "max_tokens": 1200, "timeout": 180},
    "ceiling": {"concurrency": 48, "per_level": 48, "max_tokens": 1200, "timeout": 180},
}


def apply_preset(name: str, args: argparse.Namespace) -> None:
    p = PRESETS[name]
    args.concurrency = p["concurrency"]
    args.per_level = p["per_level"]
    args.max_tokens = p["max_tokens"]
    args.timeout = p["timeout"]


def warn_per_level(conc: int, per_level: int) -> None:
    if per_level < conc * 2:
        print(
            f"WARNING: --per-level {per_level} < concurrency×2 ({conc * 2}). "
            f"CAPACITY.zh.md recommends ≥ concurrency×2 (×3 for slow models) "
            f"or TPM will be underestimated.\n"
        )


def main() -> int:
    load_dotenv_if_present()

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--preset",
        choices=sorted(PRESETS),
        default=PRESET,
        help=f"apply CAPACITY.zh.md parameter set (default {PRESET})",
    )
    ap.add_argument("-c", "--concurrency", type=int, default=CONCURRENCY,
                    help=f"fixed concurrency per round (default {CONCURRENCY}; overridden by --preset)")
    ap.add_argument("--per-level", type=int, default=PER_LEVEL,
                    help=f"requests per round (default {PER_LEVEL})")
    ap.add_argument("--max-tokens", type=int, default=MAX_TOKENS,
                    help=f"max_tokens per request (default {MAX_TOKENS})")
    ap.add_argument("--timeout", type=int, default=TIMEOUT,
                    help=f"per-request timeout seconds (default {TIMEOUT})")
    ap.add_argument("-d", "--duration", type=int, default=DURATION,
                    help=f"total measured time in seconds (default {DURATION})")
    ap.add_argument("--cooldown", type=int, default=COOLDOWN,
                    help=f"seconds between rounds; use ≥70 after 429/breaker (default {COOLDOWN})")
    ap.add_argument("--stop-error-rate", type=float, default=STOP_ERROR_RATE,
                    help=f"stop after a round exceeds this error rate (default {STOP_ERROR_RATE})")
    ap.add_argument("--max-rounds", type=int, default=0,
                    help="optional cap on number of rounds (0 = unlimited until duration)")
    args = ap.parse_args()

    apply_preset(args.preset, args)

    require("TF_GATEWAY_URL")
    require("TF_VIRTUAL_KEY")

    if args.concurrency < 1 or args.per_level < 1 or args.duration < 1:
        sys.exit("ERROR: concurrency, per-level, and duration must be >= 1")

    url, headers, model = gateway_from_env()
    body = make_body(model, args.max_tokens)

    warn_per_level(args.concurrency, args.per_level)

    print(f"target : {url}")
    print(f"model  : {model}   max_tokens={args.max_tokens}")
    print(f"preset : {args.preset}   concurrency={args.concurrency}   "
          f"requests/round={args.per_level}")
    print(f"duration: {args.duration}s   cooldown={args.cooldown}s\n")
    print(RESULT_HDR)
    print("-" * len(RESULT_HDR))

    rows: list[dict] = []
    t_end = time.monotonic() + args.duration
    round_no = 0

    try:
        while time.monotonic() < t_end:
            round_no += 1
            r = run_level(url, headers, body, args.concurrency, args.per_level, args.timeout)
            rows.append(r)
            print(format_result_row(r))
            if r["sample_err"]:
                print(f"     err sample: {r['sample_err'][:110]}")
            print(f"     round {round_no}  wall={r['wall']:.1f}s")

            if error_rate(r) > args.stop_error_rate:
                print(f"\n>>> error rate {error_rate(r):.0%} > {args.stop_error_rate:.0%} "
                      f"— stopping sustain run")
                break
            if args.max_rounds and round_no >= args.max_rounds:
                break
            if time.monotonic() + args.cooldown >= t_end:
                break
            time.sleep(args.cooldown)
    except KeyboardInterrupt:
        print("\n>>> interrupted — printing partial results")

    if not rows:
        print("\nNo rounds completed.")
        return 1

    print_peak_summary(rows)

    # Best *clean* round (zero 429/503) — matches how CAPACITY.zh.md picks sweet spots.
    clean = [r for r in rows if r["c429"] == 0 and r["c503"] == 0]
    if clean:
        best = max(clean, key=lambda r: r["tpm"])
        print(f"\nBest clean round (0×429, 0×503): TPM={best['tpm']:.0f}  RPS={best['rps']:.2f}")
    else:
        print("\nNo clean round (all rounds had 429 or 503).")

    return 0


if __name__ == "__main__":
    sys.exit(main())
