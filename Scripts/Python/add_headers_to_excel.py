import re
import json
from pathlib import Path
import xlrd, openpyxl, xlwt
import pandas as pd
from pyspark.sql import SparkSession

# .option("dataAddress", "'Sheet1'!A1") \ 
# print(spark._jvm.scala.util.Properties.versionString())
# print(spark._jvm.org.apache.spark.SPARK_VERSION)
# print(spark.version)


spark = SparkSession.builder.master("local[*]") \
    .appName("InjectHeadersXLS") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.13:3.5.1_0.20.4") \
    .getOrCreate()

inventory = pd.read_csv("Remaining/file_inventory_xls_rem.csv", dtype=str).fillna("")
failed = []

for _, row in inventory.iterrows():
    if row["file_type"].upper() not in ("XLS", "XLSX"):
        continue

    gen_headers = row["generated_headers"].strip()
    schema      = row["schema"].strip()
    file_path   = row["file_path"].strip()
    filename    = row["filename"].strip()

    # Decision
    if not gen_headers or gen_headers == "[]":
        if schema and schema != "[]":
            print(f"schema exists → {filename}")
        continue

    try:
        headers = json.loads(gen_headers)

        sdf = spark.read \
            .format("com.crealytics.spark.excel") \
            .option("header", "false") \
            .option("inferSchema", "false") \
            .load(file_path)

        actual  = len(sdf.columns)
        headers = (headers + [f"col_{i}" for i in range(actual)])[:actual]
        sdf     = sdf.toDF(*headers)

        pandas_df = sdf.toPandas()

        # Strip illegal characters that openpyxl can't handle
        pandas_df = pandas_df.apply(
            lambda col: col.map(
                lambda v: re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", str(v))
                if isinstance(v, str) else v
            )
        )

        out_path = "Organized Data/XLSX/" + Path(file_path).stem + ".xlsx"
        pandas_df.to_excel(out_path, index=False, engine="openpyxl")
        print(f"done → {out_path}")

        inventory.at[_, "file_path"] = out_path
        inventory.at[_, "file_type"] = "XLSX"

    except openpyxl.utils.exceptions.IllegalCharacterError as e:
        print(f"[ILLEGAL CHAR] {filename}: {e}")
        failed.append((filename, str(e)))

    except Exception as e:
        print(f"[ERROR] {filename}: {e}")
        failed.append((filename, str(e)))

spark.stop()

inventory.to_csv("Remaining/file_inventory_xls_rem.csv", index=False)
print("Inventory updated.")

if failed:
    print(f"\nFailed files ({len(failed)}):")
    for name, reason in failed:
        print(f"  {name}: {reason}")