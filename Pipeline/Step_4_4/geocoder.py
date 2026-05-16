import asyncio
import aiohttp
import logging
from src.config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HEREGeocoder:
    def __init__(self):
        self.api_key = Config.HERE_API_KEY
        self.url = "https://geocode.search.hereapi.com/v1/geocode"
        self.concurrency_limit = Config.GEO_CONCURRENCY
        self.semaphore = asyncio.Semaphore(self.concurrency_limit)

    async def _fetch_one(self, session, address, addr_hash):
        async with self.semaphore:
            params = {
                "q": address,
                "apiKey": self.api_key
            }
            
            retries = 0
            while retries <= Config.MAX_RETRIES:
                try:
                    async with session.get(self.url, params=params, timeout=10) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data.get("items"):
                                pos = data["items"][0]["position"]
                                return {
                                    "hash": addr_hash,
                                    "normalized_address": address,
                                    "lat": f"{pos['lat']:.{Config.COORD_PRECISION}f}",
                                    "lon": f"{pos['lng']:.{Config.COORD_PRECISION}f}",
                                    "status": "SUCCESS"
                                }
                            else:
                                return {
                                    "hash": addr_hash,
                                    "normalized_address": address,
                                    "lat": None,
                                    "lon": None,
                                    "status": "NOT_FOUND"
                                }
                        
                        elif response.status in [429, 500, 502, 503, 504]:
                            retries += 1
                            delay = Config.INITIAL_DELAY * (2 ** (retries - 1))
                            logger.warning(f"Status {response.status} for {address}. Retrying in {delay}s... ({retries}/{Config.MAX_RETRIES})")
                            await asyncio.sleep(delay)
                        else:
                            logger.error(f"Status {response.status} for {address}. Giving up.")
                            return {
                                "hash": addr_hash,
                                "normalized_address": address,
                                "lat": None,
                                "lon": None,
                                "status": f"ERROR_{response.status}"
                            }
                except Exception as e:
                    retries += 1
                    delay = Config.INITIAL_DELAY * (2 ** (retries - 1))
                    logger.warning(f"Error for {address}: {e}. Retrying in {delay}s... ({retries}/{Config.MAX_RETRIES})")
                    await asyncio.sleep(delay)
            
            return {
                "hash": addr_hash,
                "normalized_address": address,
                "lat": None,
                "lon": None,
                "status": "TIMEOUT_OR_FAILED"
            }

    async def geocode_batch(self, batch_data):
        # batch_data: list of (normalized_address, hash)
        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch_one(session, addr, h) for addr, h in batch_data]
            results = await asyncio.gather(*tasks)
            return results
