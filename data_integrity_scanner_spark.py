"""
data_integrity_scanner_spark.py
─────────────────────────────────────────────────────────────────────────────
Spark-based version of data_integrity_scanner.py.
Reads a master CSV and computes the same integrity metrics using PySpark
for CSV/TSV/JSON payloads, with Excel fallback via pandas.

Usage:
  python data_integrity_scanner_spark.py master.csv
  python data_integrity_scanner_spark.py master.csv output.csv
"""

import csv
import json
import math
import os
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

NULLISH = {"", "nan", "none", "null", "na", "n/a", "#n/a", "nil", "-", "--"}


def is_null(val) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    return str(val).strip().lower() in NULLISH


def infer_type(val) -> str:
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


def clean_text_column(col):
    return F.regexp_replace(F.lower(F.trim(F.coalesce(F.col(col).cast("string"), F.lit("")))), "[, ]", "")


def nullish_expr(col):
    cleaned = clean_text_column(col)
    return (
        F.col(col).isNull()
        | (cleaned == "")
        | cleaned.isin(*NULLISH)
    )


def type_expr(col):
    cleaned = clean_text_column(col)
    return F.when(nullish_expr(col), F.lit("null"))
    return F.when(nullish_expr(col), F.lit("null"))


def infer_type_expr(col):
    cleaned = clean_text_column(col)
    return F.when(nullish_expr(col), F.lit("null")) \
        .when(cleaned.rlike(r"^-?\d+$"), F.lit("int")) \
        .when(cleaned.rlike(r"^-?\d+(\.\d+)?$"), F.lit("float")) \
        .otherwise(F.lit("string"))


def calc_null_pct(df):
    if isinstance(df, pd.DataFrame):
        cols = df.columns
        if len(cols) == 0:
            return 0.0
        null_count = sum(is_null(v) for col in cols for v in df[col])
        total_cells = df.size
        return round((null_count / total_cells) * 100, 3) if total_cells else 0.0

    cols = df.columns
    if len(cols) == 0:
        return 0.0

    null_counts = [F.sum(F.when(nullish_expr(c), 1).otherwise(0)).alias(c) for c in cols]
    row = df.select(*null_counts).collect()[0]
    null_total = sum(row[c] for c in cols)
    total_cells = df.count() * len(cols)
    return round((null_total / total_cells) * 100, 3) if total_cells else 0.0


def calc_type_deviation(df):
    if isinstance(df, pd.DataFrame):
        cols = df.columns
        if len(cols) == 0:
            return 0.0

        total_violations = 0
        total_non_null = 0
        for col in cols:
            types = [infer_type(v) for v in df[col]]
            non_null = [t for t in types if t != "null"]
            if not non_null:
                continue
            modal_type = Counter(non_null).most_common(1)[0][0]
            violations = sum(1 for t in non_null if t != modal_type)
            total_violations += violations
            total_non_null += len(non_null)

        return round((total_violations / total_non_null) * 100, 3) if total_non_null else 0.0

    cols = df.columns
    if len(cols) == 0:
        return 0.0

    total_violations = 0
    total_non_null = 0

    for c in cols:
        typed = df.select(infer_type_expr(c).alias(c))
        non_null = typed.filter(F.col(c) != "null")
        count_by_type = non_null.groupBy(c).count().orderBy(F.desc("count")).collect()
        if not count_by_type:
            continue
        modal_type = count_by_type[0][c]
        modal_count = count_by_type[0]["count"]
        non_null_count = non_null.count()
        violations = non_null_count - modal_count
        total_violations += violations
        total_non_null += non_null_count

    return round((total_violations / total_non_null) * 100, 3) if total_non_null else 0.0


def calc_entropy_delta(df):
    if isinstance(df, pd.DataFrame):
        cols = df.columns
        if len(cols) == 0:
            return 0.0

        deltas = []
        for c in cols:
            vals = [str(v).strip().lower() for v in df[c] if not is_null(v)]
            if len(vals) < 2:
                continue
            counts = Counter(vals)
            n = len(vals)
            h = -sum((count / n) * math.log2(count / n) for count in counts.values() if count > 0)
            h_max = math.log2(len(counts)) if len(counts) > 1 else 1.0
            deltas.append(abs((h / h_max) - 1) * 100)

        return round(sum(deltas) / len(deltas), 3) if deltas else 0.0

    cols = df.columns
    if len(cols) == 0:
        return 0.0

    deltas = []
    for c in cols:
        non_null = df.filter(~nullish_expr(c)).select(F.lower(F.trim(F.coalesce(F.col(c).cast("string"), F.lit("")))).alias("value"))
        if non_null.count() < 2:
            continue
        counts = non_null.groupBy("value").count().collect()
        total = sum(row["count"] for row in counts)
        if total == 0:
            continue
        h = -sum((row["count"] / total) * math.log2(row["count"] / total) for row in counts)
        h_max = math.log2(len(counts)) if len(counts) > 1 else 1.0
        deltas.append(abs((h / h_max) - 1) * 100)

    return round(sum(deltas) / len(deltas), 3) if deltas else 0.0


def calc_field_deviation_csv(spark, file_path, delimiter, has_header):
    try:
        with open(file_path, "r", errors="replace") as fh:
            reader = csv.reader(fh, delimiter=delimiter)
            lengths = [len(row) for i, row in enumerate(reader) if not (has_header and i == 0)]
    except Exception:
        return 0.0, 0

    if not lengths:
        return 0.0, 0

    modal = Counter(lengths).most_common(1)[0][0]
    wrong = sum(1 for length in lengths if length != modal)
    return round((wrong / len(lengths)) * 100, 3), len(lengths)


def process_csv(spark, file_path, has_header):
    delimiter = detect_delimiter(file_path)
    field_deviation_pct, total_rows = calc_field_deviation_csv(spark, file_path, delimiter, has_header)
    try:
        df = spark.read.option("header", "true" if has_header else "false") \
            .option("sep", delimiter) \
            .option("inferSchema", "false") \
            .option("encoding", "UTF-8") \
            .option("mode", "PERMISSIVE") \
            .csv(file_path)
    except Exception as exc:
        return _empty_metrics(str(exc))

    return {
        "null_pct": calc_null_pct(df),
        "field_deviation_pct": field_deviation_pct,
        "type_deviation_pct": calc_type_deviation(df),
        "entropy_delta_pct": calc_entropy_delta(df),
        "total_rows": df.count(),
        "total_cells": df.count() * len(df.columns),
        "error": "",
    }


def process_tsv(spark, file_path, has_header):
    try:
        df = spark.read.option("header", "true" if has_header else "false") \
            .option("sep", "\t") \
            .option("inferSchema", "false") \
            .option("encoding", "UTF-8") \
            .option("mode", "PERMISSIVE") \
            .csv(file_path)
    except Exception as exc:
        return _empty_metrics(str(exc))

    field_deviation_pct, total_rows = calc_field_deviation_csv(spark, file_path, "\t", has_header)
    return {
        "null_pct": calc_null_pct(df),
        "field_deviation_pct": field_deviation_pct,
        "type_deviation_pct": calc_type_deviation(df),
        "entropy_delta_pct": calc_entropy_delta(df),
        "total_rows": df.count(),
        "total_cells": df.count() * len(df.columns),
        "error": "",
    }


def process_json(spark, file_path):
    try:
        df = spark.read.option("multiLine", "true").json(file_path)
        if df.rdd.isEmpty():
            return {
                "null_pct": 0.0,
                "field_deviation_pct": 0.0,
                "type_deviation_pct": 0.0,
                "entropy_delta_pct": 0.0,
                "total_rows": 0,
                "total_cells": 0,
                "error": "",
            }
    except Exception:
        # fallback to pandas JSON normalization for more complex JSON roots
        try:
            with open(file_path, "r", errors="replace") as fh:
                data = json.load(fh)
        except Exception as exc:
            return _empty_metrics(str(exc))

        if isinstance(data, list) and all(isinstance(item, dict) for item in data):
            df = pd.json_normalize(data).astype(str)
        elif isinstance(data, dict):
            df = pd.json_normalize(data).astype(str)
        else:
            return _empty_metrics("Unsupported JSON root type")

        return {
            "null_pct": calc_null_pct(pd.DataFrame(df)),
            "field_deviation_pct": 0.0,
            "type_deviation_pct": calc_type_deviation(pd.DataFrame(df)),
            "entropy_delta_pct": calc_entropy_delta(pd.DataFrame(df)),
            "total_rows": len(df),
            "total_cells": df.size,
            "error": "",
        }

    return {
        "null_pct": calc_null_pct(df),
        "field_deviation_pct": 0.0,
        "type_deviation_pct": calc_type_deviation(df),
        "entropy_delta_pct": calc_entropy_delta(df),
        "total_rows": df.count(),
        "total_cells": df.count() * len(df.columns),
        "error": "",
    }


def process_excel(file_path, has_header, ext):
    try:
        engine = "xlrd" if ext == "xls" else "openpyxl"
        df = pd.read_excel(file_path, header=0 if has_header else None, dtype=str, engine=engine, keep_default_na=False)
    except Exception as exc:
        return _empty_metrics(str(exc))

    return {
        "null_pct": calc_null_pct(df),
        "field_deviation_pct": calc_field_deviation_df(df),
        "type_deviation_pct": calc_type_deviation(df),
        "entropy_delta_pct": calc_entropy_delta(df),
        "total_rows": len(df),
        "total_cells": df.size,
        "error": "",
    }


def calc_field_deviation_df(df):
    if df.empty:
        return 0.0
    row_widths = [int(row.notna().sum()) for _, row in df.iterrows()]
    modal = Counter(row_widths).most_common(1)[0][0]
    wrong = sum(1 for w in row_widths if w != modal)
    return round((wrong / len(row_widths)) * 100, 3)


def _empty_metrics(error):
    return {
        "null_pct": None,
        "field_deviation_pct": None,
        "type_deviation_pct": None,
        "entropy_delta_pct": None,
        "total_rows": None,
        "total_cells": None,
        "error": error,
    }


def detect_delimiter(file_path: str) -> str:
    try:
        with open(file_path, "r", errors="replace") as fh:
            sample = fh.read(8192)
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t|;")
        return dialect.delimiter
    except csv.Error:
        return ","


def dispatch(spark, row):
    file_path = str(row.get("file_path", "")).strip()
    file_type = str(row.get("file_type", "")).strip().lower().lstrip(".")
    raw_header = str(row.get("has_header", "true")).strip().lower()
    has_header = raw_header in ("true", "1", "yes", "y")

    if not file_path:
        return _empty_metrics("file_path is empty")
    if not os.path.exists(file_path):
        return _empty_metrics(f"File not found: {file_path}")

    if file_type in ("csv", ""):
        return process_csv(spark, file_path, has_header)
    if file_type == "tsv":
        return process_tsv(spark, file_path, has_header)
    if file_type in ("xlsx", "xls"):
        return process_excel(file_path, has_header, file_type)
    if file_type == "json":
        return process_json(spark, file_path)

    return process_csv(spark, file_path, has_header)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    master_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else (Path(master_path).stem + "_with_metrics.csv")

    if not os.path.exists(master_path):
        print(f"[ERROR] Master CSV not found: {master_path}")
        sys.exit(1)

    # Ensure Spark uses the same Python executable and binds locally on Windows.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    spark = SparkSession.builder \
        .appName("DataIntegrityScannerSpark") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable) \
        .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", sys.executable) \
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
        .config("spark.python.worker.faulthandler.enabled", "true") \
        .getOrCreate()

    master_df = spark.read.option("header", "true").option("inferSchema", "false").csv(master_path)
    master_df = master_df.toPandas()

    if "file_path" not in master_df.columns or "file_type" not in master_df.columns:
        print("[ERROR] Master CSV must contain file_path and file_type columns")
        spark.stop()
        sys.exit(1)

    master_df.columns = master_df.columns.str.lower().str.strip()
    metrics = []
    total = len(master_df)
    print(f"\nScanning {total} file(s) using Spark…\n")

    for idx, row in master_df.iterrows():
        fname = row.get("filename", row.get("file_path", f"row {idx}"))
        ftype = row.get("file_type", "?")
        print(f"  [{idx + 1:>4}/{total}]  {ftype:<6}  {fname}")
        metrics.append(dispatch(spark, row.to_dict()))

    metrics_df = pd.DataFrame(metrics)
    master_df = pd.concat([master_df.reset_index(drop=True), metrics_df.reset_index(drop=True)], axis=1)
    master_df.to_csv(output_path, index=False)

    print(f"\nOutput → {output_path}\n")
    spark.stop()


if __name__ == "__main__":
    main()
