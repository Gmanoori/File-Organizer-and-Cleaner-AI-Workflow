import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

def inline_clean_csv(directory):
    # Initialize Spark
    spark = SparkSession.builder \
            .appName("InlineHTMLCleaner") \
            .config("spark.sql.debug.maxToStringFields", "1000") \
            .getOrCreate()
    
    # Temporary directory for Spark output
    temp_root = os.path.join(directory, "spark_temp_workdir")
        
    # Get list of all CSV files
    files = [f for f in os.listdir(directory) if f.endswith('.csv')]

    for file_name in files:
        original_path = os.path.join(directory, file_name)
        temp_output_path = os.path.join(temp_root, file_name.replace(".csv", ""))
        
        print(f"Processing: {file_name}")
        
        try:
            # We read as 'text' instead of 'csv' to preserve the raw line structure.
            # This prevents HTML tags containing commas from breaking the CSV schema.
            df = spark.read.text(original_path)
            
            # Replace all HTML tags <...> with a "."
            # The regex <[^>]+> matches opening tags, closing tags, and self-closing tags
            cleaned_df = df.withColumn("value", regexp_replace(col("value"), r"<[^>]+>", ""))
    
            # Write to temporary folder
            cleaned_df.coalesce(1).write.mode("overwrite").text(temp_output_path)
    
            # Find the actual part-file Spark created (it will end in .txt because we used .text())
            part_file = [f for f in os.listdir(temp_output_path) if f.startswith("part-")][0]
            temp_file_full_path = os.path.join(temp_output_path, part_file)
    
            # Perform the 'Replace'
            # 1. Remove original
            os.remove(original_path)
            # 2. Move temp part-file to original location with original name
            shutil.move(temp_file_full_path, original_path)
            # Cleanup the individual temp folder
            shutil.rmtree(temp_output_path)
        except Exception as e:
            print(f"Error processing {file_name}: {e}")
    
        # Final cleanup of the temp root
    if os.path.exists(temp_root):
        shutil.rmtree(temp_root)
    
    spark.stop()
    print("Cleanup complete. All files replaced.")
  
if __name__ == "__main__":
# Update this path to your directory
    TARGET_DIR = r"C:\programs\700GB Cleaning Shi\Sample\Sorted\csv\cleaned"
    inline_clean_csv(TARGET_DIR)