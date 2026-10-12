"""Block -> UTC time lookup from real Polygon block times (brief decision 17).

    python scripts/06_block_times.py anchors    # real time of every 1,000th block
    python scripts/06_block_times.py validate   # exact times for 500 random blocks vs interpolation
    python scripts/06_block_times.py months     # first block of each true UTC month (exact)

Outputs in D:\\polymarket-economy-data\\lookup:
    block_anchors.csv   block_number, timestamp (unix seconds, UTC) for every 1,000th block
    month_bounds.csv    month, first_block, first_block_ts
and D:\\polymarket-economy-data\\eda\\block_time_validation.csv

Resumable: each step appends as it goes and skips what is already on disk.
Endpoint: PublicNode's free public Polygon RPC. Header-only requests, small batches,
a pause between requests, and exponential backoff on errors.
"""
import argparse
import bisect
import csv
import json
import random
import statistics
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

RPC_URL = "https://polygon-bor-rpc.publicnode.com"
BATCH = 20
PAUSE_SECONDS = 0.5
MAX_TRIES = 8

DATA_ROOT = Path(r"D:\polymarket-economy-data")
LOOKUP_DIR = DATA_ROOT / "lookup"
ANCHORS = LOOKUP_DIR / "block_anchors.csv"
MONTHS = LOOKUP_DIR / "month_bounds.csv"
VALIDATION = DATA_ROOT / "eda" / "block_time_validation.csv"

# Block range of the dataset, from the trades and users footers (test A).
FIRST_BLOCK, LAST_BLOCK = 35_896_869, 94_968_832
STEP = 1000
FIRST_ANCHOR = FIRST_BLOCK // STEP * STEP
LAST_ANCHOR = -(-LAST_BLOCK // STEP) * STEP
FIRST_MONTH, LAST_MONTH = (2022, 11), (2026, 11)
N_VALIDATION, SEED = 500, 20261012


def rpc_timestamps(blocks: list[int]) -> dict[int, int]:
    """Real timestamps for a batch of blocks, with retries and backoff."""
    payload = [{"jsonrpc": "2.0", "id": b, "method": "eth_getHeaderByNumber", "params": [hex(b)]}
               for b in blocks]
    req = urllib.request.Request(
        RPC_URL, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"})
    for attempt in range(1, MAX_TRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                out = json.load(resp)
            got = {o["id"]: int(o["result"]["timestamp"], 16) for o in out}
            if set(got) == set(blocks):
                time.sleep(PAUSE_SECONDS)
                return got
            problem = f"{len(got)} of {len(blocks)} results"
        except Exception as exc:  # network errors, rate limits, malformed or error responses
            problem = repr(exc)
        wait = min(2 ** attempt, 120)
        print(f"  retry {attempt}/{MAX_TRIES} in {wait}s: {problem}", flush=True)
        time.sleep(wait)
    raise SystemExit(f"RPC failed {MAX_TRIES} times for blocks starting {blocks[0]}. Re-run to resume.")


def read_pairs(path: Path) -> dict[int, int]:
    if not path.exists():
        return {}
    with path.open(newline="", encoding="utf-8") as f:
        return {int(r["block_number"]): int(r["timestamp"]) for r in csv.DictReader(f)}


def append_rows(path: Path, header: list[str], rows: list[list]) -> None:
    new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if new:
            writer.writerow(header)
        writer.writerows(rows)


def load_anchors() -> dict[int, int]:
    anchors = read_pairs(ANCHORS)
    missing = [b for b in range(FIRST_ANCHOR, LAST_ANCHOR + 1, STEP) if b not in anchors]
    if missing:
        raise SystemExit(f"{len(missing):,} anchors missing. Run the 'anchors' step first.")
    return anchors


def interpolate(anchors: dict[int, int], block: int) -> float:
    a0 = block // STEP * STEP
    if a0 == block:
        return float(anchors[a0])
    return anchors[a0] + (block - a0) * (anchors[a0 + STEP] - anchors[a0]) / STEP


def fmt(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def step_anchors() -> None:
    done = read_pairs(ANCHORS)
    todo = [b for b in range(FIRST_ANCHOR, LAST_ANCHOR + 1, STEP) if b not in done]
    total = (LAST_ANCHOR - FIRST_ANCHOR) // STEP + 1
    print(f"Anchors: {total:,} needed, {len(done):,} on disk, {len(todo):,} to fetch from {RPC_URL}",
          flush=True)
    for i in range(0, len(todo), BATCH):
        blocks = todo[i:i + BATCH]
        got = rpc_timestamps(blocks)
        append_rows(ANCHORS, ["block_number", "timestamp"], [[b, got[b]] for b in blocks])
        if (i // BATCH) % 100 == 0:
            print(f"  {len(done) + i + len(blocks):,} / {total:,}", flush=True)

    anchors = load_anchors()
    ordered = sorted(anchors.items())
    backwards = sum(1 for (_, t0), (_, t1) in zip(ordered, ordered[1:]) if t1 <= t0)
    paces = [(t1 - t0) / STEP for (_, t0), (_, t1) in zip(ordered, ordered[1:])]
    print(f"Done: {len(anchors):,} anchors, blocks {ordered[0][0]:,} to {ordered[-1][0]:,}")
    print(f"Time range (UTC): {fmt(ordered[0][1])} to {fmt(ordered[-1][1])}")
    print(f"Anchor pairs where time does not increase: {backwards}")
    print(f"Seconds per block between anchors: min {min(paces):.3f}, "
          f"median {statistics.median(paces):.3f}, max {max(paces):.3f}")


def step_validate() -> None:
    anchors = load_anchors()
    rng = random.Random(SEED)
    sample = []
    while len(sample) < N_VALIDATION:
        b = rng.randint(FIRST_BLOCK, LAST_BLOCK)
        if b % STEP and b not in sample:
            sample.append(b)

    actual = read_pairs(VALIDATION)
    todo = [b for b in sample if b not in actual]
    print(f"Validation: {N_VALIDATION} random blocks (seed {SEED}), {len(todo)} to fetch")
    for i in range(0, len(todo), BATCH):
        blocks = todo[i:i + BATCH]
        got = rpc_timestamps(blocks)
        append_rows(VALIDATION, ["block_number", "timestamp", "interpolated", "error_seconds"],
                    [[b, got[b], round(interpolate(anchors, b), 3),
                      round(interpolate(anchors, b) - got[b], 3)] for b in blocks])
        actual.update(got)

    errors = sorted(abs(interpolate(anchors, b) - actual[b]) for b in sample)
    worst = max(sample, key=lambda b: abs(interpolate(anchors, b) - actual[b]))
    print(f"Absolute error in seconds, interpolated vs real ({len(errors)} blocks):")
    print(f"  median {statistics.median(errors):.2f}, p95 {errors[int(0.95 * len(errors))]:.2f}, "
          f"p99 {errors[int(0.99 * len(errors))]:.2f}, max {errors[-1]:.2f}")
    print(f"  within 5 s: {sum(e <= 5 for e in errors)}, within 60 s: {sum(e <= 60 for e in errors)}")
    print(f"  worst block {worst:,}: real {fmt(actual[worst])}, interpolated {fmt(interpolate(anchors, worst))}")
    print(f"Saved to {VALIDATION}")


def first_block_at_or_after(anchors: dict[int, int], blocks: list[int], times: list[int],
                            target: int) -> tuple[int, int] | None:
    """Exact first block whose real timestamp is >= target, by bisection inside one anchor interval."""
    if target > times[-1]:
        return None
    i = bisect.bisect_left(times, target)
    if i == 0:
        return blocks[0], times[0]
    lo, hi = blocks[i - 1], blocks[i]  # time(lo) < target <= time(hi)
    hi_ts = times[i]
    while hi - lo > 1:
        mid = (lo + hi) // 2
        ts = rpc_timestamps([mid])[mid]
        if ts >= target:
            hi, hi_ts = mid, ts
        else:
            lo = mid
    return hi, hi_ts


def step_months() -> None:
    anchors = load_anchors()
    blocks = sorted(anchors)
    times = [anchors[b] for b in blocks]
    done = {}
    if MONTHS.exists():
        with MONTHS.open(newline="", encoding="utf-8") as f:
            done = {r["month"]: r for r in csv.DictReader(f)}

    (y, m) = FIRST_MONTH
    while (y, m) <= LAST_MONTH:
        label = f"{y:04d}-{m:02d}"
        if label not in done:
            target = int(datetime(y, m, 1, tzinfo=timezone.utc).timestamp())
            found = first_block_at_or_after(anchors, blocks, times, target)
            if found is None:
                print(f"{label}: starts after the last anchor ({fmt(times[-1])}); no bound written")
            else:
                append_rows(MONTHS, ["month", "first_block", "first_block_ts"], [[label, *found]])
                print(f"{label}: first block {found[0]:,} at {fmt(found[1])}", flush=True)
        y, m = (y + m // 12, m % 12 + 1)
    print(f"Saved to {MONTHS}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("step", choices=["anchors", "validate", "months"])
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    if not DATA_ROOT.exists():
        raise SystemExit(f"{DATA_ROOT} not found. Is D: mounted?")
    LOOKUP_DIR.mkdir(parents=True, exist_ok=True)
    {"anchors": step_anchors, "validate": step_validate, "months": step_months}[args.step]()


if __name__ == "__main__":
    main()
