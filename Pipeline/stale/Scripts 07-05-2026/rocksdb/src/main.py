import os
import glob
import polars as pl
import asyncio
import logging
from src.config import Config
from src.normalizer import normalize_address, generate_hash
from src.cache import CacheManager
from src.geocoder import HEREGeocoder

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def process_file(file_path, cache_mgr, geocoder):
    filename = os.path.basename(file_path)
    output_path = os.path.join(Config.OUTPUT_DIR, filename.replace(".csv", ".parquet"))
    
    logger.info(f"Processing {filename}...")
    
    # 1. Read CSV using Polars
    # We use scan_csv for lazy evaluation if possible, but for small-medium files read_csv is fine.
    # Given 1M-5M rows, read_csv is generally okay if RAM allows, otherwise we chunk.
    # For now, let's use read_csv as it's simpler for joining back.
    df = pl.read_csv(file_path)
    
    if Config.ADDRESS_COLUMN not in df.columns:
        logger.error(f"Column '{Config.ADDRESS_COLUMN}' not found in {filename}. Skipping.")
        return

    # 2. Normalize and Hash
    # We do this on unique addresses to save computation and API calls
    unique_addresses = df.select(Config.ADDRESS_COLUMN).unique()
    
    # Convert to list of dicts for processing
    addr_list = unique_addresses.to_dicts()
    
    processed_unique = []
    for row in addr_list:
        raw_addr = str(row[Config.ADDRESS_COLUMN])
        norm_addr = normalize_address(raw_addr)
        h = generate_hash(norm_addr)
        processed_unique.append({
            Config.ADDRESS_COLUMN: raw_addr,
            "normalized_address": norm_addr,
            "geo_hash": h
        })
    
    unique_df = pl.DataFrame(processed_unique)
    
    # 3. Check Cache
    hashes = unique_df["geo_hash"].to_list()
    cached_results = cache_mgr.get_cached_results(hashes)
    logger.info(f"Cache hit: {len(cached_results)} / {len(hashes)}")
    
    # 4. Identify Cache Misses
    to_geocode = []
    for row in processed_unique:
        h = row["geo_hash"]
        if h not in cached_results:
            to_geocode.append((row["normalized_address"], h))
    
    # 5. Geocode Misses
    if to_geocode:
        logger.info(f"Geocoding {len(to_geocode)} addresses...")
        # Process in sub-batches if needed, but HEREGeocoder handles concurrency via semaphore
        new_results = await geocoder.geocode_batch(to_geocode)
        
        # Save to cache
        cache_mgr.save_results(new_results)
        
        # Merge new results into cached_results dict for the join
        for res in new_results:
            cached_results[res["hash"]] = {
                "lat": res["lat"],
                "lon": res["lon"],
                "status": res["status"]
            }
    
    # 6. Join results back to unique_df
    results_list = []
    for h, res in cached_results.items():
        results_list.append({
            "geo_hash": h,
            "latitude": res["lat"],
            "longitude": res["lon"],
            "geocode_status": res["status"]
        })
    
    results_df = pl.DataFrame(results_list)
    
    # Final Join: Original DF -> unique_df (to get geo_hash) -> results_df (to get coords)
    final_df = df.join(unique_df, on=Config.ADDRESS_COLUMN, how="left")
    final_df = final_df.join(results_df, on="geo_hash", how="left")
    
    # Drop intermediate columns
    final_df = final_df.drop(["normalized_address", "geo_hash"])
    
    # 7. Write to Parquet
    os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
    final_df.write_parquet(output_path)
    logger.info(f"Saved enriched data to {output_path}")

async def main():
    if not Config.HERE_API_KEY:
        logger.error("HERE_API_KEY not found in environment variables.")
        return

    cache_mgr = CacheManager()
    geocoder = HEREGeocoder()
    
    csv_files = glob.glob(os.path.join(Config.DATA_DIR, "*.csv"))
    
    if not csv_files:
        logger.warning(f"No CSV files found in {Config.DATA_DIR}")
        return

    for file_path in csv_files:
        try:
            await process_file(file_path, cache_mgr, geocoder)
        except Exception as e:
            logger.exception(f"Failed to process {file_path}: {e}")
            
    cache_mgr.close()

if __name__ == "__main__":
    asyncio.run(main())
