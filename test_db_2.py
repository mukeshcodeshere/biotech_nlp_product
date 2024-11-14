import os
from psycopg2 import pool

# Get the connection string from the environment variable
connection_string = "postgresql://db_biotech_owner:t0KZ7pnguiFb@ep-shrill-smoke-a5xu8h2u.us-east-2.aws.neon.tech/db_biotech?sslmode=require"
# Create a connection pool
connection_pool = pool.SimpleConnectionPool(
    1,  # Minimum number of connections in the pool
    10,  # Maximum number of connections in the pool
    connection_string
)
# Check if the pool was created successfully
if connection_pool:
    print("Connection pool created successfully")
# Get a connection from the pool
conn = connection_pool.getconn()
# Create a cursor object
cur = conn.cursor()
# Execute SQL commands to retrieve the current time and version from PostgreSQL
cur.execute('SELECT NOW();')
time = cur.fetchone()[0]
cur.execute('SELECT version();')
version = cur.fetchone()[0]
# Close the cursor and return the connection to the pool
cur.close()
connection_pool.putconn(conn)
# Close all connections in the pool
connection_pool.closeall()
# Print the results
print('Current time:', time)
print('PostgreSQL version:', version)