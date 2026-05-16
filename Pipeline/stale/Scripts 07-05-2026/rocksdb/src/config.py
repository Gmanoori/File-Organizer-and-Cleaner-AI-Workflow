import os
from dotenv import load_dotenv

# Load .env from two levels above the project root
# Assuming project root is where docker-compose.yml is
# But for local iterative execution (not in docker), we might need to look up.
# For docker, the env_file is already loaded into the environment.
# We call load_dotenv just in case for local runs.
load_dotenv("../../.env")

class Config:
    HERE_API_KEY = os.getenv("HERE_API_KEY")
    ADDRESS_COLUMN = os.getenv("ADDRESS_COLUMN", "address")
    GEO_CONCURRENCY = int(os.getenv("GEO_CONCURRENCY", "10"))
    GEO_BATCH_SIZE = int(os.getenv("GEO_BATCH_SIZE", "1000"))
    
    DATA_DIR = "data"
    STORAGE_DIR = "storage"
    OUTPUT_DIR = "output"
    
    DUCKDB_CACHE_PATH = os.path.join(STORAGE_DIR, "geo_cache.duckdb")
    
    # Retry settings
    MAX_RETRIES = 5
    INITIAL_DELAY = 1.0  # seconds
    
    # Precision
    COORD_PRECISION = 6
