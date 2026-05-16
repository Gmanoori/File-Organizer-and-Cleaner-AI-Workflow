import sys
from rocksdict import Rdict

ROCKSDB_PATH = "storage/rocks_db"

def lookup(key):
    db = Rdict(ROCKSDB_PATH)
    val = db.get(key)
    db.close()
    
    if val:
        print(f"Found record for {key}:")
        print(val)
    else:
        print(f"No record found for {key}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python src/lookup.py <phone_number>")
    else:
        lookup(sys.argv[1])
