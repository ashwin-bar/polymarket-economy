# Databricks notebook source
# Test E, step 1: can Databricks serverless compute reach Hugging Face?
#
# Tries to read the footer of markets.parquet (main branch) four ways and reports exactly what
# each one returns or raises. Transfers a few KB only. Imported into the workspace and run as a
# one-time serverless job by the laptop; the result comes back through dbutils.notebook.exit.

# COMMAND ----------

import json
import struct
import subprocess
import sys
import traceback
import urllib.request

REPO = "SII-WANGZJ/Polymarket_data"
FILE = "markets.parquet"
API_URL = f"https://huggingface.co/api/datasets/{REPO}/revision/main"
FILE_URL = f"https://huggingface.co/datasets/{REPO}/resolve/main/{FILE}"
EXPECTED_ROWS = 4_049_900
results = {}


def attempt(name, fn):
    try:
        results[name] = {"ok": True, "result": fn()}
    except Exception as exc:
        results[name] = {"ok": False, "error_type": type(exc).__name__, "error": str(exc)[:1500],
                         "traceback_tail": traceback.format_exc()[-1500:]}


def api_call():
    with urllib.request.urlopen(API_URL, timeout=60) as resp:
        return {"status": resp.status, "sha": json.load(resp).get("sha")}


def https_footer():
    """Range requests on the resolve URL (redirects to the HF CDN), then parse the footer with pyarrow."""
    import io
    import pyarrow.parquet as pq

    def get_range(spec):
        req = urllib.request.Request(FILE_URL, headers={"Range": f"bytes={spec}"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read(), resp.status, resp.geturl().split("?")[0]

    tail, status, final_url = get_range("-8")
    if tail[4:] != b"PAR1":
        raise ValueError(f"unexpected tail {tail!r}")
    footer_len = struct.unpack("<I", tail[:4])[0]
    footer, _, _ = get_range(f"-{footer_len + 8}")
    out = {"status": status, "served_from": final_url[:80], "footer_bytes": footer_len,
           "bytes_fetched": len(footer)}
    try:
        # pyarrow can parse a footer-only buffer if it is presented as the end of a file
        meta = pq.read_metadata(io.BytesIO(b"PAR1" + footer))
        out.update(num_rows=meta.num_rows, num_row_groups=meta.num_row_groups,
                   rows_match_expected=meta.num_rows == EXPECTED_ROWS)
    except Exception as exc:  # the network part worked; only the local parse failed
        out["parse_error"] = f"{type(exc).__name__}: {exc}"[:500]
    return out


def pip_install(package):
    out = subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", package],
                         capture_output=True, text=True, timeout=600)
    if out.returncode != 0:
        raise RuntimeError(f"pip install {package} failed: {out.stderr[-800:]}")


def hf_fsspec():
    """hf:// through huggingface_hub's fsspec filesystem."""
    pip_install("huggingface_hub")
    import pyarrow.parquet as pq
    from huggingface_hub import HfFileSystem

    with HfFileSystem().open(f"datasets/{REPO}@main/{FILE}", "rb") as f:
        meta = pq.ParquetFile(f).metadata
        return {"num_rows": meta.num_rows, "rows_match_expected": meta.num_rows == EXPECTED_ROWS}


def duckdb_hf():
    """hf:// through DuckDB's httpfs extension (also needs extensions.duckdb.org)."""
    pip_install("duckdb")
    import duckdb

    rows = duckdb.sql(
        f"SELECT num_rows FROM parquet_file_metadata('hf://datasets/{REPO}@main/{FILE}')").fetchone()[0]
    return {"duckdb_version": duckdb.__version__, "num_rows": rows,
            "rows_match_expected": rows == EXPECTED_ROWS}


attempt("1_https_api_call", api_call)
attempt("2_https_range_footer", https_footer)
attempt("3_hf_fsspec", hf_fsspec)
attempt("4_duckdb_hf", duckdb_hf)
results["python"] = sys.version.split()[0]

print(json.dumps(results, indent=2))
dbutils.notebook.exit(json.dumps(results))  # noqa: F821 (dbutils is provided by Databricks)
