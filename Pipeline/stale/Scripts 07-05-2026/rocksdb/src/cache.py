import duckdb
from datetime import datetime
from src.config import Config

class CacheManager:
    def __init__(self):
        self.conn = duckdb.connect(Config.DUCKDB_CACHE_PATH)
        self._setup()

    def _setup(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS geocode_cache (
                hash TEXT PRIMARY KEY,
                normalized_address TEXT,
                lat TEXT,
                lon TEXT,
                status TEXT,
                timestamp TIMESTAMP
            )
        """)

    def get_cached_results(self, hashes: list) -> dict:
        if not hashes:
            return {}
        
        # DuckDB handles list parameters well
        res = self.conn.execute(
            "SELECT hash, lat, lon, status FROM geocode_cache WHERE hash IN (SELECT UNNEST(?))",
            [hashes]
        ).fetchall()
        
        return {row[0]: {"lat": row[1], "lon": row[2], "status": row[3]} for row in res}

    def save_results(self, results: list):
        if not results:
            return
        
        # results: list of dicts {hash, normalized_address, lat, lon, status}
        timestamp = datetime.now()
        
        # Prepare for batch insert
        data = [
            (r['hash'], r['normalized_address'], r['lat'], r['lon'], r['status'], timestamp)
            for r in results
        ]
        
        self.conn.executemany("""
            INSERT OR REPLACE INTO geocode_cache (hash, normalized_address, lat, lon, status, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, data)

    def close(self):
        self.conn.close()
