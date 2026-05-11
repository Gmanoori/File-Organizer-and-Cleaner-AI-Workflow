import os
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import requests

# --- CONFIGURATION ---
# User can modify the model name here
MODEL_NAME = "mistralai/Mistral-7B-Instruct-v0.2" 

# Number of concurrent threads for API calls
MAX_THREADS = 10 

# Path to the .env file
ENV_FILE_PATH = ".env"
# ---------------------

def load_hf_token():
    """Loads the Hugging Face token from the specified .env file."""
    if os.path.exists(ENV_FILE_PATH):
        load_dotenv(ENV_FILE_PATH)
    else:
        load_dotenv()
    
    token = os.getenv("hf_token") or os.getenv("HF_TOKEN")
    return token

def query_huggingface(data, token, file_name, col_count):
    """Sends a query to the Hugging Face Inference API."""
    if not token:
        return {"error": "Missing HF token"}

    api_url = f"https://api-inference.huggingface.co/models/{MODEL_NAME}"
    headers = {"Authorization": f"Bearer {token}"}
    
    # Prompt updated based on user requirements
    prompt = (
        f"Look at this data from a CSV file named '{file_name}' and generate possible headers for the same. "
        f"Return exactly {col_count} headers as a comma-separated list, which matches the column count of the data.\n\n"
        f"Data (first 5 rows):\n{json.dumps(data, indent=2)}\n\n"
        "Suggested Headers:"
    )
    
    payload = {
        "inputs": prompt,
        "parameters": {
            "wait_for_model": True,
            "max_new_tokens": 150,
            "return_full_text": False
        }
    }
    
    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}

def process_file(file_path, token, spark):
    """Gathers the first 5 rows using Spark and triggers the API call."""
    file_name = os.path.basename(file_path)
    try:
        # header=False captures the very first rows, even if they are headers, 
        # so the AI can see what's currently there.
        df = spark.read.csv(file_path, header=False, inferSchema=False).limit(5)
        rows = df.collect()
        
        if not rows:
            return file_name, {"info": "File is empty."}
        
        col_count = len(df.columns)
        # Convert Spark rows to a list of lists for better representation of raw data
        data = [list(row) for row in rows]
        
        # Send to Hugging Face
        result = query_huggingface(data, token, file_name, col_count)
        return file_name, result
    except Exception as e:
        return file_name, {"error": str(e)}

def main():
    token = load_hf_token()
    if not token:
        print("Error: 'hf_token' not found in .env file.")
        return

    # User input for custom path
    if len(sys.argv) > 1:
        target_path = sys.argv[1]
    else:
        target_path = input("Enter the directory path containing CSV files: ").strip()

    if not os.path.isdir(target_path):
        print(f"Error: '{target_path}' is not a valid directory.")
        return

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("HuggingFaceHeaderGenerator") \
        .master("local[*]") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()

    # Get list of CSV files in target directory
    csv_files = [os.path.join(target_path, f) for f in os.listdir(target_path) 
                 if f.lower().endswith('.csv') and os.path.isfile(os.path.join(target_path, f))]
    
    # Filter out empty files
    csv_files = [f for f in csv_files if os.path.getsize(f) > 0]
    
    if not csv_files:
        print(f"No valid CSV files found in '{target_path}'.")
        spark.stop()
        return

    print(f"Found {len(csv_files)} files. Starting multi-threaded processing with {MAX_THREADS} threads...")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(process_file, f, token, spark): f for f in csv_files}
        
        for future in as_completed(futures):
            file_path, response = future.result()
            file_name = os.path.basename(file_path)
            print(f"\n[FILE: {file_name}]")
            
            if isinstance(response, list) and len(response) > 0 and 'generated_text' in response[0]:
                print(response[0]['generated_text'].strip())
            else:
                print(json.dumps(response, indent=2))
            print("-" * 50)

    print("\nProcessing complete.")
    spark.stop()

if __name__ == "__main__":
    main()
