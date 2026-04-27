"""
data_validation_checker.py
===========================
Stage 2.5  —  sits between data_integrity_scanner and add_schema_to_csv
Drop this file into:  Scripts/Python/data_validation_checker.py

Four-Tier Architecture
-----------------------
  Tier 1 : Detect suspicious rows          (statistical heuristics)
  Tier 2 : Deterministic repair            (CleverCSV + column-shift brute force)
  Tier 3 : LLM escalation                 (same gemini/gemma modules as add_schema)
  Tier 4 : Quarantine                      (written to a single quarantined_rows.csv)

Input inventory columns (from data_integrity_scanner output):
  serial_number | filename | file_type | file_path | null_pct |
  field_deviation_pct | type_deviation_pct | entropy_delta_pct |
  total_rows | total_cells | error

Usage
------
  # Minimal
  python Scripts/Python/data_validation_checker.py \
      --inventory Output/file_inventory_deviation.csv \
      --output-dir Output/validated

  # Full options
  python Scripts/Python/data_validation_checker.py \
      --inventory       Output/file_inventory_deviation.csv \
      --output-dir      Output/validated \
      --report-path     Output/mangled_rows_summary.csv \
      --quarantine-path Output/quarantined_rows.csv \
      --max-shift       5 \
      --llm-batch       10 \
      --use-spark

  # Detect only, no file writes
  python Scripts/Python/data_validation_checker.py \
      --inventory  Output/file_inventory_deviation.csv \
      --output-dir Output/validated \
      --dry-run

Dependencies
------------
  pip install clevercsv pandas scipy tqdm
  Optional Spark: pip install pyspark
"""

import argparse
import importlib.util
import json
import logging
import os
from platform import processor
import re
import sys
from pathlib import Path
# from wsgiref import headers

import numpy as np
import pandas as pd
from scipy import stats
from tqdm import tqdm

# ─────────────────────────────────────────────────────────────────
#  CONSTANTS  —  confirmed from your file_inventory_deviation.csv
# ─────────────────────────────────────────────────────────────────
FILE_PATH_COL      = "file_path"
FILENAME_COL       = "filename"
ENCODING           = "UTF-8"
FALLBACK_ENCODINGS = ["latin-1", "cp1252", "iso-8859-1"]

# Characters that cause row shifts in web-scraped data
SUSPICIOUS_CHARS = ["\n", "\r", "%20", "\\n", "\\r", "\x00", "\t", ",", "<br>", "<br/>", "<br />", "<b>", "</b>"]

# ─────────────────────────────────────────────────────────────────
#  TIER 1 THRESHOLDS  —  tune these if detection is too noisy/loose
# ─────────────────────────────────────────────────────────────────
AVG_HEADER_RATIO_THRESHOLD = 0.60   # flag if |non_null_fields/n_headers - 1| > this
TYPE_SHIFT_THRESHOLD       = 0.40   # flag if >40% of cells have wrong type vs column mode
FIELD_COUNT_ZSCORE         = 3.0    # flag if field-count z-score exceeds this

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# =================================================================
#  TIER 1 — DETECT
# =================================================================

def infer_type(value: str) -> str:
    """Cheap single-cell type inference."""
    v = str(value).strip()
    if v in ("", "nan", "None", "NULL", "null", "N/A"):
        return "null"
    try:
        int(v)
        return "int"
    except ValueError:
        pass
    try:
        float(v)
        return "float"
    except ValueError:
        pass
    if re.match(r"\d{4}-\d{2}-\d{2}", v) or re.match(r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}", v):
        return "date"
    return "str"


def column_type_modes(df: pd.DataFrame) -> dict:
    """Most common inferred type per column across all rows."""
    modes = {}
    for col in df.columns:
        series = df[col].astype(str).apply(infer_type)
        m = series.mode()
        modes[col] = m.iloc[0] if len(m) else "str"
    return modes


def type_shift_score(row: pd.Series, modes: dict) -> float:
    """Fraction of cells whose inferred type disagrees with the column mode type."""
    total = mismatches = 0
    for col, val in row.items():
        expected = modes.get(str(col), "str")
        actual   = infer_type(str(val))
        if expected != "null" and actual != "null":
            total += 1
            if actual != expected:
                mismatches += 1
    return mismatches / total if total else 0.0


def detect_suspicious(df: pd.DataFrame) -> dict:
    """
    Tier 1: Returns {row_index: [reasons_list]} for suspicious rows.

    Heuristics:
      1. avg_header_ratio   — non-null field count vs expected header count
      2. type_shift_score   — fraction of type mismatches across columns
      3. field_count_zscore — statistical outlier on per-row field count
      4. suspicious_chars   — embedded characters known to cause shifts
    """
    n_headers = len(df.columns)
    modes     = column_type_modes(df)

    counts  = df.notna().sum(axis=1).astype(float).values
    zscores = np.abs(stats.zscore(counts)) if counts.std() > 0 else np.zeros(len(counts))

    suspicious = {}

    for i in range(len(df)):
        row     = df.iloc[i]
        reasons = []

        # Heuristic 1: avg_header_ratio
        non_null = sum(1 for v in row if str(v).strip() not in ("", "nan", "None"))
        ahr = non_null / n_headers if n_headers else 1.0
        if abs(ahr - 1.0) > AVG_HEADER_RATIO_THRESHOLD:
            reasons.append(f"avg_header_ratio={ahr:.3f}")

        # Heuristic 2: type_shift
        ts = type_shift_score(row, modes)
        if ts > TYPE_SHIFT_THRESHOLD:
            reasons.append(f"type_shift={ts:.3f}")

        # Heuristic 3: field count z-score
        z = zscores[i]
        if z > FIELD_COUNT_ZSCORE:
            reasons.append(f"field_count_zscore={z:.2f}")

        # Heuristic 4: suspicious embedded characters
        found = list({sc for val in row for sc in SUSPICIOUS_CHARS if sc in str(val)})
        if found:
            reasons.append(f"suspicious_chars={found}")

        if reasons:
            suspicious[i] = reasons

    return suspicious


# =================================================================
#  TIER 2 — DETERMINISTIC REPAIR  (CleverCSV + shift brute-force)
# =================================================================

def clevercsv_reparse(raw_line: str, n_headers: int):
    """Try CleverCSV dialect sniffing on the raw line. Returns list or None."""
    try:
        import clevercsv
        dialect = clevercsv.Sniffer().sniff(raw_line, verbose=False)
        if dialect is None:
            return None
        for parsed in clevercsv.reader([raw_line], dialect):
            if len(parsed) == n_headers:
                return list(parsed)
    except Exception:
        pass
    return None


def shift_candidates(row_vals: list, n_headers: int, max_shift: int):
    """
    Generator: yields (repaired_list, label) for every column-shift
    that makes len(repaired_list) == n_headers.
    Tries smallest shifts first.
    """
    cur = len(row_vals)
    for s in range(1, max_shift + 1):
        if cur - s == n_headers:
            yield row_vals[s:],   f"drop_left_{s}"
            yield row_vals[:-s],  f"drop_right_{s}"
        if cur + s == n_headers:
            yield row_vals + [""] * s, f"pad_right_{s}"
            yield [""] * s + row_vals, f"pad_left_{s}"


def tier2_repair(raw_line: str, row_vals: list, n_headers: int, max_shift: int):
    """
    Returns (repaired_values, method_label) or (None, "tier2_failed").
    Order:
      2a. Raw line via CleverCSV
      2b. Suspicious-char-stripped line via CleverCSV
      2c. Column-shift brute force (smallest shift wins)
    """
    # 2a
    result = clevercsv_reparse(raw_line, n_headers)
    if result:
        return result, "clevercsv_raw"

    # 2b
    cleaned = raw_line
    for sc in SUSPICIOUS_CHARS:
        cleaned = cleaned.replace(sc, " ")
    if cleaned != raw_line:
        result = clevercsv_reparse(cleaned, n_headers)
        if result:
            return result, "clevercsv_stripped"

    # 2c
    for repaired, label in shift_candidates(row_vals, n_headers, max_shift):
        return repaired, f"shift:{label}"

    return None, "tier2_failed"


# =================================================================
#  TIER 3 — LLM ESCALATION
# =================================================================

def load_llm():
    api_choice  = os.environ.get("API_CHOICE", "gemma").lower()
    script_dir  = Path(__file__).parent
    module_name = f"call_{api_choice}_chat"
    module_path = script_dir / f"{module_name}.py"

    if not module_path.exists():
        log.warning(
            f"LLM module not found: {module_path}. "
            "Tier 3 will be skipped. Set API_CHOICE=gemini or gemma."
        )
        return None, None

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    fn = getattr(mod, module_name, None)   # e.g. call_gemma_chat or call_gemini_chat
    if fn is None:
        log.warning(f"Function '{module_name}' not found in {module_path}. Check function name.")
        return None, None

    log.info(f"Loaded LLM: {module_name}()")
    return mod, fn


def build_repair_prompt(headers: list, bad_rows: list) -> str:
    """
    Prompt for the LLM to repair shifted/mangled CSV rows.
    Follows the same direct, JSON-only style as add_schema_to_csv.py.
    """
    n          = len(headers)
    header_str = ", ".join(headers)
    rows_block = "\n".join(f"  ROW_{i}: {r}" for i, r in enumerate(bad_rows))

    return f"""You are a CSV data repair expert.

The CSV file has these {n} column headers (in order):
{header_str}

The following rows are malformed — possible causes include column shifts,
embedded newlines, URL-encoded characters like %20, or web scraping artifacts:

{rows_block}

For each ROW_N, return a JSON object with:
  "row_id"    : the ROW_N label (e.g. "ROW_0")
  "repaired"  : a list of exactly {n} field values in the correct column order,
                or null if the row is unresolvable
  "confidence": "high" | "medium" | "low"

Return ONLY a JSON array. No markdown, no explanation, no extra text.
Example: [{{"row_id":"ROW_0","repaired":["v1","v2","v3"],"confidence":"high"}}]"""


def llm_repair_batch(headers: list, bad_rows: list, llm_fn, batch_size: int) -> dict:
    if llm_fn is None:
        return {i: (None, "llm_unavailable") for i in range(len(bad_rows))}

    results = {}
    for start in range(0, len(bad_rows), batch_size):
        batch  = bad_rows[start: start + batch_size]
        prompt = build_repair_prompt(headers, batch)
        try:
            messages = [{"role": "user", "content": prompt}]
            raw = llm_fn(messages)
            raw = re.sub(r"```(?:json)?|```", "", raw).strip()
            parsed = json.loads(raw)
            for item in parsed:
                idx        = int(item["row_id"].replace("ROW_", "")) + start
                repaired   = item.get("repaired")
                confidence = item.get("confidence", "unknown")
                if isinstance(repaired, list) and len(repaired) == len(headers):
                    results[idx] = (repaired, confidence)
                else:
                    results[idx] = (None, confidence)
        except Exception as e:
            log.warning(f"LLM batch {start}–{start+len(batch)-1} failed: {e}")
            for i in range(len(batch)):
                results[start + i] = (None, "llm_error")

    return results

# =================================================================
#  CORE FILE PROCESSOR
# =================================================================

def read_excel(path: str):
    """Read XLS (xlrd) or XLSX (openpyxl) into (df, raw_lines)."""
    path_lower = path.lower()
    try:
        if path_lower.endswith(".xls"):
            # xlrd only supports legacy .xls
            df = pd.read_excel(path, dtype=str, engine="xlrd")
        else:
            # openpyxl for .xlsx
            df = pd.read_excel(path, dtype=str, engine="openpyxl")
        df = df.fillna("").astype(str)
        # Excel has no raw text lines — Tier 2a/2b will skip, 2c (shift) still runs
        raw_lines = [""] * (len(df) + 1)   # +1 to account for header offset
        return df, raw_lines
    except Exception as e:
        raise ValueError(f"Could not read Excel file {path}: {e}")


def read_json(path: str):
    """Read JSON into (df, raw_lines). Handles records and list-of-dicts."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            raw = json.load(f)

        # Normalise: could be a list of dicts, or a dict with a records key
        if isinstance(raw, list):
            df = pd.DataFrame(raw).astype(str).fillna("")
        elif isinstance(raw, dict):
            # Try to find the first key whose value is a list (common API response shape)
            records_key = next((k for k, v in raw.items() if isinstance(v, list)), None)
            if records_key:
                df = pd.DataFrame(raw[records_key]).astype(str).fillna("")
            else:
                df = pd.DataFrame([raw]).astype(str).fillna("")
        else:
            raise ValueError("Unrecognised JSON structure — expected list or dict at root")

        raw_lines = [""] * (len(df) + 1)
        return df, raw_lines
    except Exception as e:
        raise ValueError(f"Could not read JSON file {path}: {e}")


def read_csv(path: str):
    """Read CSV trying multiple encodings. Returns (df, raw_lines)."""
    for enc in [ENCODING] + FALLBACK_ENCODINGS:
        try:
            df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding=enc, on_bad_lines="skip")
            with open(path, errors="replace") as f:
                lines = f.readlines()
            return df, lines
        except Exception as e:
            log.debug(f"  Encoding failed: {e}")
            continue
        raise ValueError(f"Could not read file with any known encoding: {path}")


def read_input_df(path: str):
    """
    Format router — dispatches to the right reader based on file extension.
    Always returns (df, raw_lines).
    raw_lines is empty-string-padded for non-CSV formats (Tier 2a/2b won't fire,
    but Tier 2c shift repair and Tier 3 LLM still work normally).
    """
    path_lower = str(path).lower()

    if path_lower.endswith((".xlsx", ".xls")):
        return read_excel(path)

    if path_lower.endswith(".json"):
        return read_json(path)

    # CSV — try encodings in order
    return read_csv(path)

    raise ValueError(f"Could not read file with any known encoding: {path}")

def process_file(filename: str, file_path: str, args, llm_fn):
    """
    Run all four tiers on one data CSV.
    Returns:
      cleaned_df         — repaired DataFrame (quarantined rows removed)
      report_records     — rows for mangled_rows_summary.csv
      quarantine_records — rows for quarantined_rows.csv
    """
    report_records    = []
    quarantine_records = []

    try:
        df, raw_lines = read_input_df(file_path)
    except Exception as e:
        log.error(f"  Cannot read {file_path}: {e}")
        report_records.append({
            "filename": filename, "file_path": file_path,
            "row_number": -1, "tier_resolved": "read_error",
            "reason": str(e), "status": "ERROR",
        })
        return pd.DataFrame(), report_records, quarantine_records

    headers   = list(df.columns)
    n_headers = len(headers)

    # ── TIER 1: Detect ────────────────────────────────────────────
    suspicious = detect_suspicious(df)
    if not suspicious:
        return df, [], []

    log.info(f"  Tier 1 → {len(suspicious)} suspicious rows detected")

    cleaned_df = df.copy()
    needs_llm  = []   # (df_idx, raw_line, reasons)

    # ── TIER 2: Deterministic repair ──────────────────────────────
    for idx, reasons in suspicious.items():
        raw_line_idx = idx + 1          # line 0 = header
        raw_line     = (
            raw_lines[raw_line_idx].rstrip("\n")
            if raw_line_idx < len(raw_lines) else ""
        )
        row_vals = list(df.iloc[idx].values)

        if args.dry_run:
            report_records.append({
                "filename": filename, "file_path": file_path,
                "row_number": idx + 1, "tier_resolved": "dry_run",
                "reason": "; ".join(reasons), "status": "DETECTED",
            })
            continue

        repaired, method = tier2_repair(raw_line, row_vals, n_headers, args.max_shift)

        if repaired is not None:
            cleaned_df.iloc[idx] = repaired
            report_records.append({
                "filename": filename, "file_path": file_path,
                "row_number": idx + 1, "tier_resolved": f"tier2:{method}",
                "reason": "; ".join(reasons), "status": "REPAIRED",
            })
        else:
            needs_llm.append((idx, raw_line, reasons))

    # ── TIER 3: LLM escalation ────────────────────────────────────
    to_quarantine = []   # (df_idx, reasons)

    if needs_llm and not args.dry_run:
        log.info(f"  Tier 3 → escalating {len(needs_llm)} rows to LLM")
        raw_for_llm = [raw for _, raw, _ in needs_llm]
        llm_results = llm_repair_batch(headers, raw_for_llm, llm_fn, args.llm_batch)

        for local_i, (df_idx, _, reasons) in enumerate(needs_llm):
            repaired, confidence = llm_results.get(local_i, (None, "missing"))
            if repaired is not None:
                cleaned_df.iloc[df_idx] = repaired
                report_records.append({
                    "filename": filename, "file_path": file_path,
                    "row_number": df_idx + 1,
                    "tier_resolved": f"tier3:llm(conf={confidence})",
                    "reason": "; ".join(reasons), "status": "REPAIRED",
                })
            else:
                to_quarantine.append((df_idx, reasons))

    # ── TIER 4: Quarantine ────────────────────────────────────────
    quarantine_indices = []
    for df_idx, reasons in to_quarantine:
        quarantine_indices.append(df_idx)
        row_dict = dict(zip(headers, cleaned_df.iloc[df_idx].values))
        quarantine_records.append({
            "filename": filename, "file_path": file_path,
            "row_number": df_idx + 1,
            "reason": "; ".join(reasons),
            **row_dict,
        })
        report_records.append({
            "filename": filename, "file_path": file_path,
            "row_number": df_idx + 1, "tier_resolved": "tier4:quarantined",
            "reason": "; ".join(reasons), "status": "QUARANTINED",
        })

    if quarantine_indices:
        log.info(f"  Tier 4 → quarantining {len(quarantine_indices)} rows")
        cleaned_df = cleaned_df.drop(index=quarantine_indices).reset_index(drop=True)

    return cleaned_df, report_records, quarantine_records


# =================================================================
#  OPTIONAL SPARK WRAPPER
# =================================================================

def process_file_spark(filename: str, file_path: str, args, llm_fn):
    """
    Spark variant: uses Spark only to load the file (good for very large CSVs).
    Tiers 1-4 run in pandas after converting the Spark DataFrame — this is intentional
    because row-level repair doesn't benefit from distributed execution.
    Falls back to plain pandas if PySpark is unavailable.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        log.warning("PySpark not installed — falling back to pandas.")
        return process_file(filename, file_path, args, llm_fn)

    try:
        spark = (
            SparkSession.builder
            .appName("DataValidationChecker")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        sdf = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(file_path)
        )
        df = sdf.toPandas().fillna("")
        spark.stop()
    except Exception as e:
        log.warning(f"Spark load failed for {file_path}: {e} — falling back to pandas.")
        return process_file(filename, file_path, args, llm_fn)

    # Read raw lines for Tier 2 (Spark doesn't give us these)
    try:
        with open(file_path, encoding=ENCODING, errors="replace") as f:
            raw_lines = f.readlines()
    except Exception:
        raw_lines = []

    # From here the logic is identical to process_file — just swap the df source
    headers   = list(df.columns)
    n_headers = len(headers)

    suspicious = detect_suspicious(df)
    if not suspicious:
        return df, [], []

    log.info(f"  Tier 1 (Spark) → {len(suspicious)} suspicious rows")

    cleaned_df       = df.copy()
    report_records   = []
    quarantine_records = []
    needs_llm        = []

    for idx, reasons in suspicious.items():
        raw_line_idx = idx + 1
        raw_line = raw_lines[raw_line_idx].rstrip("\n") if raw_line_idx < len(raw_lines) else ""
        row_vals = list(df.iloc[idx].values)

        if args.dry_run:
            report_records.append({
                "filename": filename, "file_path": file_path,
                "row_number": idx + 1, "tier_resolved": "dry_run",
                "reason": "; ".join(reasons), "status": "DETECTED",
            })
            continue

        repaired, method = tier2_repair(raw_line, row_vals, n_headers, args.max_shift)
        if repaired is not None:
            cleaned_df.iloc[idx] = repaired
            report_records.append({
                "filename": filename, "file_path": file_path,
                "row_number": idx + 1, "tier_resolved": f"tier2:{method}",
                "reason": "; ".join(reasons), "status": "REPAIRED",
            })
        else:
            needs_llm.append((idx, raw_line, reasons))

    if needs_llm and not args.dry_run:
        log.info(f"  Tier 3 (Spark path) → escalating {len(needs_llm)} rows to LLM")
        llm_results = llm_repair_batch(
            headers, [r for _, r, _ in needs_llm], llm_fn, args.llm_batch
        )
        quarantine_indices = []
        for li, (df_idx, _, reasons) in enumerate(needs_llm):
            repaired, confidence = llm_results.get(li, (None, "missing"))
            if repaired is not None:
                cleaned_df.iloc[df_idx] = repaired
                report_records.append({
                    "filename": filename, "file_path": file_path,
                    "row_number": df_idx + 1,
                    "tier_resolved": f"tier3:llm(conf={confidence})",
                    "reason": "; ".join(reasons), "status": "REPAIRED",
                })
            else:
                quarantine_indices.append(df_idx)
                row_dict = dict(zip(headers, cleaned_df.iloc[df_idx].values))
                quarantine_records.append({
                    "filename": filename, "file_path": file_path,
                    "row_number": df_idx + 1, "reason": "; ".join(reasons), **row_dict,
                })
                report_records.append({
                    "filename": filename, "file_path": file_path,
                    "row_number": df_idx + 1, "tier_resolved": "tier4:quarantined",
                    "reason": "; ".join(reasons), "status": "QUARANTINED",
                })
        if quarantine_indices:
            log.info(f"  Tier 4 (Spark path) → quarantining {len(quarantine_indices)} rows")
            cleaned_df = cleaned_df.drop(index=quarantine_indices).reset_index(drop=True)

    return cleaned_df, report_records, quarantine_records


# =================================================================
#  MAIN ORCHESTRATOR
# =================================================================

def run(args):
    log.info("=" * 55)
    log.info("Stage 2.5 — Data Validation & Row-Shift Repair")
    log.info("=" * 55)

    inv_path = Path(args.inventory)
    if not inv_path.exists():
        log.error(f"Inventory not found: {inv_path}")
        sys.exit(1)

    inventory = pd.read_csv(inv_path, dtype=str, keep_default_na=False)
    for col in [FILE_PATH_COL, FILENAME_COL]:
        if col not in inventory.columns:
            log.error(f"Column '{col}' not in inventory. Got: {list(inventory.columns)}")
            sys.exit(1)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    Path(args.report_path).parent.mkdir(parents=True, exist_ok=True)
    Path(args.quarantine_path).parent.mkdir(parents=True, exist_ok=True)

    _, llm_fn = load_llm()
    processor = process_file_spark if args.use_spark else process_file

    all_reports    = []
    all_quarantine = []

    files = inventory[[FILENAME_COL, FILE_PATH_COL]].drop_duplicates()
    for _, row in tqdm(files.iterrows(), total=len(files), desc="Validating files"):
        fname = row[FILENAME_COL]
        fpath = row[FILE_PATH_COL]

        if not Path(fpath).exists():
            log.warning(f"Skipping missing file: {fpath}")
            continue

        log.info(f"Processing: {fname}")
        cleaned_df, report_recs, q_recs = processor(fname, fpath, args, llm_fn)

        all_reports.extend(report_recs)
        all_quarantine.extend(q_recs)

        if args.dry_run or cleaned_df.empty:
            continue

        out_file = out_dir / Path(fpath).name
        cleaned_df.to_csv(out_file, index=False, encoding=ENCODING)
        log.info(f"  → Cleaned file: {out_file}")

    # ── Write mangled_rows_summary.csv ────────────────────────────
    report_df = pd.DataFrame(all_reports, columns=[
        "filename", "file_path", "row_number", "tier_resolved", "reason", "status",
    ])
    report_df.to_csv(args.report_path, index=False, encoding=ENCODING)
    log.info(f"\nMangled rows summary → {args.report_path}  ({len(report_df)} rows logged)")

    # ── Write quarantined_rows.csv ────────────────────────────────
    if all_quarantine:
        q_df      = pd.DataFrame(all_quarantine)
        front     = ["filename", "file_path", "row_number", "reason"]
        other     = [c for c in q_df.columns if c not in front]
        q_df      = q_df[front + other]
        q_df.to_csv(args.quarantine_path, index=False, encoding=ENCODING)
        log.info(f"Quarantined rows     → {args.quarantine_path}  ({len(q_df)} rows)")
    else:
        log.info("Quarantined rows     → none")

    # ── Update inventory to point to validated files ───────────────
    if not args.dry_run and not args.skip_inventory_update:
        updated_inv = inventory.copy()
        updated_inv[FILE_PATH_COL] = updated_inv[FILE_PATH_COL].apply(
            lambda p: str(out_dir / Path(p).name) if Path(p).exists() else p
        )
        updated_inv_path = out_dir / inv_path.name
        updated_inv.to_csv(updated_inv_path, index=False, encoding=ENCODING)
        log.info(f"Updated inventory    → {updated_inv_path}")
        log.info(f"\nNext step:")
        log.info(f"  python Scripts/Python/add_schema_to_csv.py {updated_inv_path}")

    # ── Tier summary ──────────────────────────────────────────────
    if not report_df.empty:
        log.info("\n── Result Summary ──────────────────────────")
        for status, grp in report_df.groupby("status"):
            log.info(f"  {status:<15} : {len(grp)}")

    log.info("\nStage 2.5 complete.")


# =================================================================
#  CLI
# =================================================================

def parse_args():
    p = argparse.ArgumentParser(
        description="Stage 2.5 — Data Validation & Row-Shift Repair",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--inventory", required=True,
        help="Path to file_inventory_deviation.csv (Stage 2 output)"
    )
    p.add_argument(
        "--output-dir", default="Output/validated",
        help="Directory for cleaned CSV files"
    )
    p.add_argument(
        "--report-path", default="Output/mangled_rows_summary.csv",
        help="Path for the mangled rows summary CSV"
    )
    p.add_argument(
        "--quarantine-path", default="Output/quarantined_rows.csv",
        help="Path for the single flat quarantined rows CSV"
    )
    p.add_argument(
        "--max-shift", type=int, default=5,
        help="Max columns to try shifting left/right in Tier 2"
    )
    p.add_argument(
        "--llm-batch", type=int, default=10,
        help="Number of rows per LLM API call in Tier 3"
    )
    p.add_argument(
        "--use-spark", action="store_true",
        help="Load files via Spark (falls back to pandas if PySpark unavailable)"
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Detect and report suspicious rows only — no files written"
    )
    p.add_argument(
        "--skip-inventory-update", action="store_true",
        help="Do not write the updated inventory CSV pointing to validated files"
    )
    return p.parse_args()


if __name__ == "__main__":
    run(parse_args())