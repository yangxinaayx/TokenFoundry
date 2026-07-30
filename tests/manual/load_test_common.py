"""Shared helpers for Token Foundry manual load tests (ramp / sustain / verify).

Metric definitions match ``docs/CAPACITY.zh.md`` and ``load_test_ramp.py``:

    RPS = successful requests / wall-clock of the batch
    TPM = total tokens of successful calls / wall * 60

Status codes:
    429  upstream Copilot throttle (account quota ceiling)
    503  APIM circuit breaker (single-hub pool, ~60s trip)
    other  timeouts, connection errors, other HTTP codes
"""

from __future__ import annotations

import json
import os
import statistics
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, wait

PROMPT = "Write a short paragraph about the ocean."

RESULT_HDR = (
    f"{'conc':>4} {'ok':>4} {'429':>4} {'503':>4} {'err':>4} "
    f"{'RPS':>7} {'TPM':>9} {'p50 s':>7} {'p95 s':>7}"
)

NOTE_FOOTER = (
    "Note: 429 = upstream Copilot throttle (the real ceiling); "
    "503 = APIM circuit breaker open (single-hub pool, clears in ~60s)."
)


def load_dotenv_if_present() -> None:
    for path in (".env", os.path.join(os.path.dirname(__file__), "..", "..", ".env")):
        if not os.path.isfile(path):
            continue
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
        return


def require(name: str) -> None:
    import sys

    v = os.environ.get(name)
    if not v:
        sys.exit(
            f"ERROR: env var {name} is required (see docstring). "
            f"Set it inline or put it in the repo-root .env"
        )


def one_call(url: str, headers: dict, body: dict, timeout: int) -> dict:
    t0 = time.perf_counter()
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            dt = time.perf_counter() - t0
            try:
                u = json.loads(raw).get("usage", {}) or {}
            except json.JSONDecodeError:
                u = {}
            tot = int(u.get("total_tokens", 0) or 0)
            if not tot:
                tot = int(u.get("input_tokens", 0) or 0) + int(u.get("output_tokens", 0) or 0)
            return {"status": r.status, "sec": dt, "tokens": tot}
    except urllib.error.HTTPError as e:
        dt = time.perf_counter() - t0
        body_txt = ""
        try:
            body_txt = e.read()[:120].decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            pass
        return {"status": e.code, "sec": dt, "tokens": 0, "err": body_txt}
    except Exception as e:  # noqa: BLE001
        return {"status": 0, "sec": time.perf_counter() - t0, "tokens": 0, "err": type(e).__name__}


def aggregate_results(results: list[dict], conc: int, wall: float) -> dict:
    """Turn raw per-call records into the standard stats row (ramp / verify / sustain)."""
    ok = [r for r in results if r["status"] == 200]
    c429 = [r for r in results if r["status"] == 429]
    c503 = [r for r in results if r["status"] == 503]
    other = [r for r in results if r["status"] not in (200, 429, 503)]
    lat = sorted(r["sec"] for r in ok)
    tokens = sum(r["tokens"] for r in ok)
    n = len(results)
    return {
        "conc": conc,
        "n": n,
        "wall": wall,
        "ok": len(ok),
        "c429": len(c429),
        "c503": len(c503),
        "other": len(other),
        "rps": len(ok) / wall if wall else 0,
        "tpm": tokens / wall * 60 if wall else 0,
        "tokens": tokens,
        "p50": statistics.median(lat) if lat else 0,
        "p95": lat[int(len(lat) * 0.95)] if len(lat) > 1 else (lat[0] if lat else 0),
        "sample_err": next((r.get("err", "") for r in results if r.get("err")), ""),
    }


def run_level(url: str, headers: dict, body: dict, conc: int, n: int, timeout: int) -> dict:
    """Batch mode (ramp): fire exactly ``n`` requests, wall = time until all finish."""
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=conc) as ex:
        results = list(ex.map(lambda _: one_call(url, headers, body, timeout), range(n)))
    wall = time.perf_counter() - t0
    return aggregate_results(results, conc, wall)


def run_pipeline(
    url: str, headers: dict, body: dict, conc: int, duration: float, timeout: int
) -> dict:
    """Pipeline mode (verify): keep ``conc`` requests in flight for ``duration`` seconds.

    Unlike ``run_level`` (submit a fixed batch then wait for drain), workers start a
    new call as soon as the previous one finishes — steady-state throughput.  Only
    completions whose timestamp falls inside the measurement window are counted;
    ``wall`` is the fixed window length (not batch drain time).
    """
    import threading

    results: list[dict] = []
    lock = threading.Lock()
    stop = threading.Event()
    t0 = time.perf_counter()
    t_end = t0 + duration

    def worker() -> None:
        while not stop.is_set():
            rec = one_call(url, headers, body, timeout)
            finished = time.perf_counter()
            if finished <= t_end:
                with lock:
                    results.append(rec)

    with ThreadPoolExecutor(max_workers=conc) as ex:
        futures = [ex.submit(worker) for _ in range(conc)]
        time.sleep(duration)
        stop.set()
        wait(futures, timeout=timeout + 10)

    return aggregate_results(results, conc, duration)


def format_result_row(r: dict) -> str:
    return (
        f"{r['conc']:>4} {r['ok']:>4} {r['c429']:>4} {r['c503']:>4} {r['other']:>4} "
        f"{r['rps']:>7.2f} {r['tpm']:>9.0f} {r['p50']:>7.2f} {r['p95']:>7.2f}"
    )


def print_peak_summary(rows: list[dict]) -> None:
    best_rps = max((r["rps"] for r in rows), default=0)
    best_tpm = max((r["tpm"] for r in rows), default=0)
    print("\n" + "=" * 60)
    print(f"PEAK sustained RPS : {best_rps:.2f} req/s")
    print(f"PEAK sustained TPM : {best_tpm:.0f} tokens/min")
    print("=" * 60)
    print(NOTE_FOOTER)


def print_best_clean(rows: list[dict]) -> None:
    """Best row with zero upstream throttle / breaker errors — CAPACITY.zh.md sweet spot."""
    clean = [r for r in rows if r["c429"] == 0 and r["c503"] == 0]
    if not clean:
        print("\nNo clean row (every step had 429 or 503).")
        return
    best = max(clean, key=lambda r: r["tpm"])
    print(
        f"\nBest clean step: conc={best['conc']}  TPM={best['tpm']:.0f}  "
        f"RPS={best['rps']:.2f}  (0×429, 0×503)"
    )
    print("CAPACITY.zh.md reference (1 GitHub account, gpt-4o-mini): ~41,600 TPM @ 24 concurrent")


def gateway_from_env() -> tuple[str, dict[str, str], str]:
    """Return (url, headers, default_model) from TF_* env vars."""
    import sys

    gw = os.environ.get("TF_GATEWAY_URL", "").rstrip("/")
    vk = os.environ.get("TF_VIRTUAL_KEY", "")
    if not gw or not vk:
        sys.exit("ERROR: TF_GATEWAY_URL and TF_VIRTUAL_KEY are required.")
    path = os.environ.get("TF_PATH", "/llm-openai/v1/chat/completions")
    auth = os.environ.get("TF_AUTH_HEADER", "api-key")
    model = os.environ.get("TF_MODEL", "gpt-4o-mini")
    url = gw + path
    headers = {auth: vk, "Content-Type": "application/json"}
    if "messages" in path:
        headers["anthropic-version"] = "2023-06-01"
    return url, headers, model


def make_body(model: str, max_tokens: int, prompt: str = PROMPT) -> dict:
    return {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }


def error_rate(r: dict) -> float:
    return 1 - (r["ok"] / r["n"] if r["n"] else 0)
