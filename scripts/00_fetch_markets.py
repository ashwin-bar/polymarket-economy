"""Test 0, step 1: fetch markets.parquet from the Hugging Face main branch.

Plain HTTPS download, so the local file is a byte-for-byte copy of the source.
Verified against the SHA-256 and size Hugging Face publishes for the file.
Resumable: skips the download if a verified copy is already on disk, and
continues a partial download from where it stopped.
"""
import hashlib
import http.client
import json
import urllib.request
from pathlib import Path

import duckdb

REPO = "SII-WANGZJ/Polymarket_data"
FILENAME = "markets.parquet"
TREE_URL = f"https://huggingface.co/api/datasets/{REPO}/tree/main"
FILE_URL = f"https://huggingface.co/datasets/{REPO}/resolve/main/{FILENAME}"
DATA_ROOT = Path(r"D:\polymarket-economy-data")
TARGET = DATA_ROOT / "raw" / FILENAME
EXPECTED_ROWS = 4_049_900  # dataset card figure, brief section 4.1
CHUNK = 8 * 1024 * 1024
MAX_ATTEMPTS = 10


def published_sha256_and_size() -> tuple[str, int]:
    with urllib.request.urlopen(TREE_URL) as resp:
        entries = json.load(resp)
    entry = next(e for e in entries if e["path"] == FILENAME)
    return entry["lfs"]["oid"], entry["lfs"]["size"]


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def download(url: str, dest: Path, expected_size: int) -> None:
    """Download to dest, resuming from a partial file with HTTP Range requests.

    The connection can drop mid-file without raising, so loop until the size matches.
    """
    for attempt in range(1, MAX_ATTEMPTS + 1):
        have = dest.stat().st_size if dest.exists() else 0
        if have == expected_size:
            return
        if have > expected_size:
            dest.unlink()
            have = 0
        print(f"  attempt {attempt}: starting at byte {have:,}")
        req = urllib.request.Request(url, headers={"Range": f"bytes={have}-"})
        try:
            with urllib.request.urlopen(req) as resp, dest.open("ab") as f:
                if resp.status != 206:
                    raise SystemExit(f"Expected HTTP 206 for a range request, got {resp.status}")
                while chunk := resp.read(CHUNK):
                    f.write(chunk)
        except (OSError, http.client.HTTPException) as exc:
            print(f"  connection dropped: {exc!r}")
    if dest.stat().st_size != expected_size:
        raise SystemExit(f"Download incomplete after {MAX_ATTEMPTS} attempts; partial file kept at {dest}")


def main() -> None:
    if not DATA_ROOT.exists():
        raise SystemExit(f"{DATA_ROOT} not found. Is D: mounted?")

    expected_sha, expected_size = published_sha256_and_size()
    print(f"Published SHA-256: {expected_sha}")
    print(f"Published size:    {expected_size:,} bytes")

    if TARGET.exists() and sha256_of(TARGET) == expected_sha:
        print(f"Verified copy already on disk, skipping download: {TARGET}")
    else:
        if TARGET.exists():
            print("File on disk does not match the published SHA-256; it will be replaced.")
        TARGET.parent.mkdir(parents=True, exist_ok=True)
        tmp = TARGET.with_suffix(".parquet.tmp")
        print(f"Downloading {FILE_URL}")
        download(FILE_URL, tmp, expected_size)
        actual_sha = sha256_of(tmp)
        if actual_sha != expected_sha:
            raise SystemExit(
                f"SHA-256 mismatch: got {actual_sha}. Kept {tmp} for inspection; "
                f"{TARGET} was not touched."
            )
        tmp.replace(TARGET)

    size = TARGET.stat().st_size
    rows = duckdb.connect().execute(
        f"SELECT count(*) FROM read_parquet('{TARGET.as_posix()}')"
    ).fetchone()[0]
    print(f"File: {TARGET}")
    print(f"Size: {size:,} bytes (match: {size == expected_size})")
    print(f"SHA-256 verified: True")
    print(f"Rows: {rows:,} (expected {EXPECTED_ROWS:,}, match: {rows == EXPECTED_ROWS})")


if __name__ == "__main__":
    main()
