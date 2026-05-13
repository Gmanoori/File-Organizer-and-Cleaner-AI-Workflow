import pandas as pd
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent
import time

# ----------------------------
# CONFIG
# ----------------------------

CSV_FILE = "C:\\Users\\GANESH\\OneDrive\\Desktop\\ScyllaDB\\people-1000000.csv"
CONCURRENCY = 2650  # ScyllaDB can handle thousands of concurrent async requests

# ----------------------------
# OPTIMIZE CONNECTION POOLING
# ----------------------------

# pooling_options = PoolingOptions()
# # Allow many more requests per connection (Protocol v3/v4 support up to 32,768)
# pooling_options.max_requests_per_connection_local = 2048
# pooling_options.max_requests_per_connection_remote = 2048
# # Adjust number of connections if needed
# pooling_options.core_connections_per_host_local = 2
# pooling_options.max_connections_per_host_local = 4

# ----------------------------
# CONNECT TO SCYLLA
# ----------------------------

cluster = Cluster(["127.0.0.1"])
session = cluster.connect("dedupe")

prepared = session.prepare("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    sex,
    email,
    phone,
    dob,
    job_title
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")

# ----------------------------
# READ CSV
# ----------------------------

df = pd.read_csv(CSV_FILE)
print(f"Loaded {len(df)} rows from CSV")

# ----------------------------
# ROW GENERATOR (More efficient than iterrows)
# ----------------------------

def get_row_params():
    count = 0
    for row in df.itertuples(index=False):
        # itertuples attributes usually replace spaces with underscores
        # but since we use index=False, we can just use integer indices if preferred,
        # or just use the named attributes. For people-1000000.csv, 
        # columns are likely: User Id, First Name, Last Name, Sex, Email, Phone, Date of birth, Job Title
        yield (
            str(getattr(row, "_0") if hasattr(row, "_0") else row[0]),
            str(getattr(row, "_1") if hasattr(row, "_1") else row[1]),
            str(getattr(row, "_2") if hasattr(row, "_2") else row[2]),
            str(getattr(row, "_3") if hasattr(row, "_3") else row[3]),
            str(getattr(row, "_4") if hasattr(row, "_4") else row[4]),
            str(getattr(row, "_5") if hasattr(row, "_5") else row[5]),
            str(getattr(row, "_6") if hasattr(row, "_6") else row[6]),
            str(getattr(row, "_7") if hasattr(row, "_7") else row[7])
        )
        
        count += 1
        if count % 1000 == 0:
            print(f"Sent {count} records...")

# ----------------------------
# START TIMER
# ----------------------------

start = time.time()

# ----------------------------
# ASYNCHRONOUS CONCURRENT INSERTS
# ----------------------------

print(f"Starting inserts with concurrency={CONCURRENCY}...")
statements_and_params = (
    (prepared, params)
    for params in get_row_params()
)

results = execute_concurrent(
    session,
    statements_and_params,
    concurrency=CONCURRENCY
)

# ----------------------------
# END TIMER
# ----------------------------

end = time.time()

print("\nDone.")
print(f"Inserted {len(df)} rows")
print(f"Time Taken: {end - start:.2f} seconds")
print(f"Throughput: {len(df) / (end - start):.2f} rows/sec")

cluster.shutdown()