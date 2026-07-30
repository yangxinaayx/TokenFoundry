#!/usr/bin/env python3
"""Token Foundry — independent TPM ceiling verification (pipeline mode).

**Purpose:** find the Copilot account TPM / RPS ceiling with the *same goal* as
``docs/CAPACITY.zh.md`` and ``load_test_ramp.py``, but using a **different load
generator** so you can cross-check results and rule out batch-specific artefacts.

+---------------------------+--------------------------------+---------------------------+
|                           | load_test_ramp.py (original)   | load_test_verify.py (this)|
+---------------------------+--------------------------------+---------------------------+
| Scheduling                | fixed batch (N requests, wait) | continuous pipeline       |
| Concurrency model         | ThreadPoolExecutor.map         | workers loop until timer  |
| Wall clock                | batch drain time               | fixed measurement window  |
| Typical use               | step conc 1→2→4→8→24→48        | step conc 16→24→32→40     |
| Output columns            | conc ok 429 503 err RPS TPM …  | **identical**             |
+---------------------------+--------------------------------+---------------------------+

Both use the same RPS/TPM formulas from CAPACITY.zh.md §2.5.  If this script's
best clean TPM at 24 concurrent lands near ~40–43k (single account), the doc
baseline is confirmed independently.

Requirements (same as ramp):
    * UNLIMITED virtual key — no ``tokens_per_minute`` on the key
    * ``TF_GATEWAY_URL`` + ``TF_VIRTUAL_KEY`` in env or repo-root ``.env``

Usage:
    # Recommended: sweep concurrency with pipeline mode (default preset)
    python tests/manual/load_test_verify.py

    # Single-point confirmation at doc sweet spot (24 concurrent, 3 min)
    python tests/manual/load_test_verify.py --preset confirm

    # Custom sweep
    python tests/manual/load_test_verify.py --levels 16,24,32,40 \\
        --duration 120 --max-tokens 1200 --timeout 180 --cooldown 30
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from load_test_common import (
    RESULT_HDR,
    error_rate,
    format_result_row,
    gateway_from_env,
    load_dotenv_if_present,
    make_body,
    print_best_clean,
    print_peak_summary,
    require,
    run_pipeline,
)

# Defaults: pipeline sweep aimed at TPM ceiling (CAPACITY.zh.md §5 equivalent goal).
PRESET = "tpm-sweep"
LEVELS = "16,24,32,40"
DURATION = 120
MAX_TOKENS = 1200
TIMEOUT = 180
COOLDOWN = 30
STOP_ERROR_RATE = 0.5

PRESETS = {
    # Sweep concurrencies — find highest clean TPM without hitting 429.
    "tpm-sweep": {
        "levels": "16,24,32,40",
        "duration": 120,
        "max_tokens": 1200,
        "timeout": 180,
        "cooldown": 30,
    },
    # Hold at the documented single-account sweet spot for a longer window.
    "confirm": {
        "levels": "24",
        "duration": 180,
        "max_tokens": 1200,
        "timeout": 180,
        "cooldown": 0,
    },
    # Push toward overload — expect 429; use cooldown >= 70 between steps.
    "overload": {
        "levels": "24,48",
        "duration": 90,
        "max_tokens": 1200,
        "timeout": 180,
        "cooldown": 70,
    },
}


def apply_preset(name: str, args: argparse.Namespace) -> None:
    p = PRESETS[name]
    args.levels = p["levels"]
    args.duration = p["duration"]
    args.max_tokens = p["max_tokens"]
    args.timeout = p["timeout"]
    args.cooldown = p["cooldown"]


def main() -> int:
    load_dotenv_if_present()

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--preset",
        choices=sorted(PRESETS),
        default=PRESET,
        help=f"parameter set (default {PRESET})",
    )
    ap.add_argument("--levels", default=LEVELS,
                    help=f"comma-separated concurrency steps (default {LEVELS})")
    ap.add_argument("--duration", type=int, default=DURATION,
                    help=f"seconds to keep pipeline busy per step (default {DURATION})")
    ap.add_argument("--max-tokens", type=int, default=MAX_TOKENS,
                    help=f"max_tokens per request (default {MAX_TOKENS})")
    ap.add_argument("--timeout", type=int, default=TIMEOUT,
                    help=f"per-request HTTP timeout (default {TIMEOUT})")
    ap.add_argument("--cooldown", type=int, default=COOLDOWN,
                    help=f"seconds between steps; use >=70 after 429/breaker (default {COOLDOWN})")
    ap.add_argument("--stop-error-rate", type=float, default=STOP_ERROR_RATE,
                    help=f"stop sweep when a step exceeds this error rate (default {STOP_ERROR_RATE})")
    args = ap.parse_args()

    apply_preset(args.preset, args)

    require("TF_GATEWAY_URL")
    require("TF_VIRTUAL_KEY")

    if args.duration < 30:
        print("WARNING: --duration < 30s may underestimate TPM on slow models.\n")

    url, headers, model = gateway_from_env()
    body = make_body(model, args.max_tokens)

    level_list = [int(x) for x in args.levels.split(",")]

    print(f"target   : {url}")
    print(f"model    : {model}   max_tokens={args.max_tokens}")
    print(f"mode     : pipeline (continuous in-flight, NOT batch map)")
    print(f"preset   : {args.preset}   levels={args.levels}   duration={args.duration}s/step\n")
    print(RESULT_HDR)
    print("-" * len(RESULT_HDR))

    rows: list[dict] = []
    try:
        for i, conc in enumerate(level_list):
            print(f"  ... pipeline conc={conc} for {args.duration}s", flush=True)
            r = run_pipeline(url, headers, body, conc, float(args.duration), args.timeout)
            rows.append(r)
            print(format_result_row(r))
            print(f"     completed={r['n']} in {r['wall']:.0f}s wall  tokens(ok)={r['tokens']}")
            if r["sample_err"]:
                print(f"     err sample: {r['sample_err'][:110]}")
            if error_rate(r) > args.stop_error_rate:
                print(f"\n>>> error rate {error_rate(r):.0%} > {args.stop_error_rate:.0%} "
                      f"— ceiling reached, stopping sweep")
                break
            if i < len(level_list) - 1 and args.cooldown > 0:
                time.sleep(args.cooldown)
    except KeyboardInterrupt:
        print("\n>>> interrupted — printing partial results")

    if not rows:
        print("\nNo steps completed.")
        return 1

    print_peak_summary(rows)
    print_best_clean(rows)
    print("\nCross-check: compare best clean TPM with load_test_ramp.py at the same concurrency.")
    print("Large divergence (>15%) may indicate breaker cooldown, key limits, or sample duration.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
