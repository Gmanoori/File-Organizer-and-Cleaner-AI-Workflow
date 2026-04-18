"""
data_integrity_scanner.py
─────────────────────────────────────────────────────────────────────────────
Reads a master CSV (serial_number, filename, file_type, file_path,
has_header, schema) and appends integrity metrics as new columns:

  null_pct            → % of null / empty / NaN cells in the file
  field_deviation_pct → Strategy 1: rows whose field count ≠ modal count
  type_deviation_pct  → Simplified Strategy 2: cells whose inferred type
                         disagrees with the column's modal type
  entropy_delta_pct   → Shannon entropy deviation from a uniform baseline
  total_rows          → data row count (excludes header)
  total_cells         → total cell count
  error               → any parse / IO error message (blank = clean)

Supported file types:  csv, tsv, xlsx, xls, json
Usage:
  python data_integrity_scanner.py master.csv
  python data_integrity_scanner.py master.csv output.csv   # custom output path
─────────────────────────────────────────────────────────────────────────────
"""

import csv
import json
import math
import os
import sys
import warnings
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=UserWarning)


# ─── helpers ──────────────────────────────────────────────────────────────────

NULLISH = {"", "nan", "none", "null", "na", "n/a", "#n/a", "nil", "-", "--"}


def is_null(val) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    return str(val).strip().lower() in NULLISH


def infer_type(val) -> str:
    """Classify a single cell value into int | float | string | null."""
    if is_null(val):
        return "null"
    s = str(val).strip()
    try:
        int(s.replace(",", "").replace(" ", ""))
        return "int"
    except ValueError:
        pass
    try:
        float(s.replace(",", ""))
        return "float"
    except ValueError:
        pass
    return "string"


def detect_delimiter(file_path: str) -> str:
    """Auto-sniff CSV delimiter; fall back to comma."""
    try:
        with open(file_path, "r", errors="replace") as fh:
            sample = fh.read(8192)
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t|;")
        return dialect.delimiter
    except csv.Error:
        return ","


# ─── metric calculators ───────────────────────────────────────────────────────

def calc_null_pct(df: pd.DataFrame) -> float:
    """% of cells that are null / empty."""
    total = df.size
    if total == 0:
        return 0.0
    null_count = sum(is_null(v) for col in df.columns for v in df[col])
    return round((null_count / total) * 100, 3)


def calc_field_deviation_csv(file_path: str, delimiter: str,
                              has_header: bool) -> tuple[float, int]:
    """
    Strategy 1 — field-count deviation.
    Reads raw lines; modal field count = expected width; deviation = rows
    whose field count differs from the mode.
    Returns (deviation_pct, total_data_rows).
    """
    lengths = []
    with open(file_path, "r", errors="replace") as fh:
        reader = csv.reader(fh, delimiter=delimiter)
        for i, row in enumerate(reader):
            if has_header and i == 0:
                continue
            lengths.append(len(row))

    if not lengths:
        return 0.0, 0

    modal = Counter(lengths).most_common(1)[0][0]
    wrong = sum(1 for ln in lengths if ln != modal)
    return round((wrong / len(lengths)) * 100, 3), len(lengths)


def calc_field_deviation_df(df: pd.DataFrame) -> float:
    """
    Strategy 1 variant for Excel / JSON (already parsed into a DataFrame).
    Uses non-null cell count per row as proxy for field width.
    """
    if df.empty:
        return 0.0
    row_widths = [int(row.notna().sum()) for _, row in df.iterrows()]
    modal = Counter(row_widths).most_common(1)[0][0]
    wrong = sum(1 for w in row_widths if w != modal)
    return round((wrong / len(row_widths)) * 100, 3)


def calc_type_deviation(df: pd.DataFrame) -> float:
    """
    Simplified Strategy 2 — within-column type consistency.
    For each column, find the modal non-null type. Count cells whose type
    differs from the mode as violations.
    Returns violation% across all non-null cells.
    """
    violations, total = 0, 0
    for col in df.columns:
        types = [infer_type(v) for v in df[col]]
        non_null = [t for t in types if t != "null"]
        if not non_null:
            continue
        modal_type = Counter(non_null).most_common(1)[0][0]
        violations += sum(1 for t in non_null if t != modal_type)
        total += len(non_null)

    if total == 0:
        return 0.0
    return round((violations / total) * 100, 3)


def calc_entropy_delta(df: pd.DataFrame) -> float:
    """
    Shannon entropy deviation.
    For each column compute H = -Σ p(x) log2 p(x).
    Uniform baseline: H_max = log2(unique_values).
    Δ = mean over columns of |H_col / H_max_col − 1| × 100.
    High Δ → skewed or injected distribution.
    """
    deltas = []
    for col in df.columns:
        vals = [str(v).strip().lower() for v in df[col] if not is_null(v)]
        if len(vals) < 2:
            continue
        counts = Counter(vals)
        n = len(vals)
        h = -sum((c / n) * math.log2(c / n) for c in counts.values() if c > 0)
        h_max = math.log2(len(counts)) if len(counts) > 1 else 1.0
        delta = abs((h / h_max) - 1) * 100
        deltas.append(delta)

    return round(sum(deltas) / len(deltas), 3) if deltas else 0.0


# ─── per-format processors ────────────────────────────────────────────────────

def _empty_metrics(error: str) -> dict:
    return dict(
        null_pct=None,
        field_deviation_pct=None,
        type_deviation_pct=None,
        entropy_delta_pct=None,
        total_rows=None,
        total_cells=None,
        error=error,
    )


def process_csv(file_path: str, has_header: bool) -> dict:
    delimiter = detect_delimiter(file_path)

    # Strategy 1 on raw text (catches injection before pandas parsing)
    field_dev, total_rows = calc_field_deviation_csv(
        file_path, delimiter, has_header
    )

    try:
        df = pd.read_csv(
            file_path,
            header=0 if has_header else None,
            sep=delimiter,
            dtype=str,
            keep_default_na=False,
            on_bad_lines="skip",
            encoding_errors="replace",
        )
    except Exception as exc:
        return _empty_metrics(str(exc))

    return dict(
        null_pct=calc_null_pct(df),
        field_deviation_pct=field_dev,
        type_deviation_pct=calc_type_deviation(df),
        entropy_delta_pct=calc_entropy_delta(df),
        total_rows=len(df),
        total_cells=df.size,
        error="",
    )


def process_excel(file_path: str, has_header: bool, ext: str) -> dict:
    try:
        engine = "xlrd" if ext == "xls" else "openpyxl"
        df = pd.read_excel(
            file_path,
            header=0 if has_header else None,
            dtype=str,
            engine=engine,
            keep_default_na=False,
        )
    except Exception as exc:
        return _empty_metrics(str(exc))

    return dict(
        null_pct=calc_null_pct(df),
        field_deviation_pct=calc_field_deviation_df(df),
        type_deviation_pct=calc_type_deviation(df),
        entropy_delta_pct=calc_entropy_delta(df),
        total_rows=len(df),
        total_cells=df.size,
        error="",
    )


def process_json(file_path: str) -> dict:
    try:
        with open(file_path, "r", errors="replace") as fh:
            data = json.load(fh)
    except Exception as exc:
        return _empty_metrics(str(exc))

    # Normalise to a DataFrame
    if isinstance(data, list):
        if not data:
            return dict(
                null_pct=0.0, field_deviation_pct=0.0,
                type_deviation_pct=0.0, entropy_delta_pct=0.0,
                total_rows=0, total_cells=0, error=""
            )
        if all(isinstance(r, dict) for r in data):
            # Strategy 1 variant: key-count consistency
            key_counts = [len(r) for r in data]
            modal = Counter(key_counts).most_common(1)[0][0]
            wrong = sum(1 for k in key_counts if k != modal)
            field_dev = round((wrong / len(key_counts)) * 100, 3)
            df = pd.json_normalize(data).astype(str)
        else:
            df = pd.DataFrame({"value": [str(v) for v in data]})
            field_dev = 0.0
    elif isinstance(data, dict):
        df = pd.json_normalize(data).astype(str)
        field_dev = 0.0
    else:
        return _empty_metrics("Unsupported JSON root type")

    return dict(
        null_pct=calc_null_pct(df),
        field_deviation_pct=field_dev,
        type_deviation_pct=calc_type_deviation(df),
        entropy_delta_pct=calc_entropy_delta(df),
        total_rows=len(df),
        total_cells=df.size,
        error="",
    )


def process_tsv(file_path: str, has_header: bool) -> dict:
    """TSV is just CSV with tab delimiter."""
    try:
        df = pd.read_csv(
            file_path,
            header=0 if has_header else None,
            sep="\t",
            dtype=str,
            keep_default_na=False,
            on_bad_lines="skip",
            encoding_errors="replace",
        )
    except Exception as exc:
        return _empty_metrics(str(exc))

    field_dev, _ = calc_field_deviation_csv(file_path, "\t", has_header)
    return dict(
        null_pct=calc_null_pct(df),
        field_deviation_pct=field_dev,
        type_deviation_pct=calc_type_deviation(df),
        entropy_delta_pct=calc_entropy_delta(df),
        total_rows=len(df),
        total_cells=df.size,
        error="",
    )


# ─── dispatcher ───────────────────────────────────────────────────────────────

def dispatch(row: dict) -> dict:
    """Route one master-CSV row to the correct processor."""
    file_path = str(row.get("file_path", "")).strip()
    file_type = str(row.get("file_type", "")).strip().lower().lstrip(".")
    raw_header = str(row.get("has_header", "true")).strip().lower()
    has_header = raw_header in ("true", "1", "yes", "y")

    if not file_path:
        return _empty_metrics("file_path is empty")
    if not os.path.exists(file_path):
        return _empty_metrics(f"File not found: {file_path}")

    try:
        if file_type in ("csv", ""):
            return process_csv(file_path, has_header)
        elif file_type == "tsv":
            return process_tsv(file_path, has_header)
        elif file_type in ("xlsx", "xls"):
            return process_excel(file_path, has_header, file_type)
        elif file_type == "json":
            return process_json(file_path)
        else:
            # Unknown type → try CSV with auto-sniffed delimiter
            return process_csv(file_path, has_header)
    except Exception as exc:
        return _empty_metrics(f"Unexpected error: {exc}")


# ─── summary printer ──────────────────────────────────────────────────────────

def print_summary(master_df: pd.DataFrame, metrics: list[dict]) -> None:
    metrics_df = pd.DataFrame(metrics)
    ok = metrics_df["error"].apply(lambda e: e == "" or pd.isna(e))
    n_ok = ok.sum()
    n_err = (~ok).sum()

    print("\n" + "─" * 55)
    print(f"  Files processed : {len(metrics_df)}")
    print(f"  Successful      : {n_ok}")
    print(f"  Errors          : {n_err}")

    if n_ok > 0:
        m = metrics_df[ok]
        print(f"\n  {'Metric':<28} {'Mean':>8}  {'Max':>8}")
        print(f"  {'─'*28} {'─'*8}  {'─'*8}")
        for col, label in [
            ("null_pct",            "Null %"),
            ("field_deviation_pct", "Field deviation %"),
            ("type_deviation_pct",  "Type deviation %"),
            ("entropy_delta_pct",   "Entropy delta %"),
        ]:
            series = pd.to_numeric(m[col], errors="coerce").dropna()
            if series.empty:
                continue
            print(f"  {label:<28} {series.mean():>7.2f}%  {series.max():>7.2f}%")

    if n_err > 0:
        print(f"\n  Files with errors:")
        err_rows = metrics_df[~ok]
        for i in err_rows.index:
            fname = master_df.at[i, "filename"] if "filename" in master_df.columns else str(i)
            print(f"    [{i}] {fname} → {metrics_df.at[i, 'error']}")
    print("─" * 55 + "\n")


# ─── entry point ──────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    master_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else (
        Path(master_path).stem + "_with_metrics.csv"
    )

    if not os.path.exists(master_path):
        print(f"[ERROR] Master CSV not found: {master_path}")
        sys.exit(1)

    master_df = pd.read_csv(master_path, dtype=str, keep_default_na=False)

    required_cols = {"file_path", "file_type"}
    missing = required_cols - set(master_df.columns.str.lower())
    if missing:
        print(f"[ERROR] Master CSV is missing columns: {missing}")
        sys.exit(1)

    # Normalise column names to lowercase for safe key access
    master_df.columns = master_df.columns.str.lower().str.strip()

    metrics_list = []
    total = len(master_df)

    print(f"\nScanning {total} file(s) …\n")
    for idx, row in master_df.iterrows():
        fname = row.get("filename", row.get("file_path", f"row {idx}"))
        ftype = row.get("file_type", "?")
        print(f"  [{idx + 1:>4}/{total}]  {ftype:<6}  {fname}")
        metrics = dispatch(row.to_dict())
        if metrics["error"]:
            print(f"            ⚠  {metrics['error']}")
        metrics_list.append(metrics)

    metrics_df = pd.DataFrame(metrics_list)

    # Drop old metric columns if rerunning on an already-enriched file
    existing_metric_cols = [
        c for c in master_df.columns
        if c in metrics_df.columns and c != "error"
    ]
    master_df.drop(columns=existing_metric_cols, inplace=True, errors="ignore")

    output_df = pd.concat(
        [master_df.reset_index(drop=True), metrics_df.reset_index(drop=True)],
        axis=1,
    )
    output_df.to_csv(output_path, index=False)
    print(f"\nOutput → {output_path}")

    print_summary(master_df, metrics_list)


if __name__ == "__main__":
    main()