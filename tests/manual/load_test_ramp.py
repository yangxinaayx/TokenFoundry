#!/usr/bin/env python3
"""Token Foundry — ramp load test: find a hub/Copilot account's RPS + TPM ceiling.

Ramps concurrency (1 -> 2 -> 4 -> 8 -> ...) and, for each level, fires a fixed
number of chat completions through the APIM gateway, measuring:

    * success / 429 (upstream throttled) / 503 (circuit breaker open) / other
    * achieved RPS  (successful requests / wall-clock of the level)
    * achieved TPM  (total tokens of successful calls, extrapolated per minute)
    * latency p50 / p95

It stops early once a level's error rate crosses a threshold — that's the
ceiling. Use an UNLIMITED virtual key (no tokens_per_minute) so you measure the
UPSTREAM (Copilot subscription) limit rather than our own llm-token-limit policy.

See ``docs/CAPACITY.zh.md`` §5 for recommended command lines and how to read results.

NO SECRETS IN THIS FILE. Configure via env:
    TF_GATEWAY_URL   https://<apim>.azure-api.net
    TF_VIRTUAL_KEY   an APIM subscription key (prefer an UNLIMITED one)
Optional:
    TF_MODEL         default gpt-4o-mini
    TF_PATH          default /llm-openai/v1/chat/completions
    TF_AUTH_HEADER   default api-key   (anthropic: x-api-key)

Usage:
    python tests/manual/load_test_ramp.py
    python tests/manual/load_test_ramp.py --levels 1,2,4,8,16 --per-level 20
    python tests/manual/load_test_ramp.py --max-tokens 200
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

LEVELS = "1,2,4,8,16"
PER_LEVEL = 12
MAX_TOKENS = 60
TIMEOUT = 90
COOLDOWN = 20
STOP_ERROR_RATE = 0.5


def main() -> int:
    load_dotenv_if_present()

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--levels", default=LEVELS,
                    help=f"comma-separated concurrency levels (default {LEVELS})")
    ap.add_argument("--per-level", type=int, default=PER_LEVEL,
                    help=f"requests per level (default {PER_LEVEL})")
    ap.add_argument("--max-tokens", type=int, default=MAX_TOKENS,
                    help=f"max_tokens per request (default {MAX_TOKENS})")
    ap.add_argument("--timeout", type=int, default=TIMEOUT,
                    help=f"per-request timeout seconds (default {TIMEOUT})")
    ap.add_argument("--cooldown", type=int, default=COOLDOWN,
                    help=f"seconds between levels; lets a tripped breaker reset "
                         f"(default {COOLDOWN})")
    ap.add_argument("--stop-error-rate", type=float, default=STOP_ERROR_RATE,
                    help=f"stop ramping once error rate exceeds this, 0-1 "
                         f"(default {STOP_ERROR_RATE})")
    args = ap.parse_args()

    require("TF_GATEWAY_URL")
    require("TF_VIRTUAL_KEY")

    url, headers, model = gateway_from_env()
    body = make_body(model, args.max_tokens)

    print(f"target : {url}")
    print(f"model  : {model}   max_tokens={args.max_tokens}")
    print(f"levels : {args.levels}   requests/level={args.per_level}\n")
    print(RESULT_HDR)
    print("-" * len(RESULT_HDR))

    rows = []
    level_list = [int(x) for x in args.levels.split(",")]
    for lvl in level_list:
        r = run_level(url, headers, body, lvl, args.per_level, args.timeout)
        rows.append(r)
        print(format_result_row(r))
        if r["sample_err"]:
            print(f"     err sample: {r['sample_err'][:110]}")
        if error_rate(r) > args.stop_error_rate:
            print(f"\n>>> error rate {error_rate(r):.0%} > {args.stop_error_rate:.0%} "
                  f"— ceiling reached, stopping ramp")
            break
        if lvl != level_list[-1]:
            time.sleep(args.cooldown)

    print_peak_summary(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
