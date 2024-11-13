import psycopg2
from psycopg2 import sql

# Replace with your full connection string
connection_string = "postgresql://db_biotech_owner:t0KZ7pnguiFb@ep-shrill-smoke-a5xu8h2u.us-east-2.aws.neon.tech/db_biotech?sslmode=require"

# Sample function to connect and execute queries
def test_neon_connection():
    try:
        # Establish a connection to the Neon database using the full connection string
        connection = psycopg2.connect(connection_string)
        cursor = connection.cursor()
        print("Successfully connected to the Neon database.")

        # Sample table name
        table_name = 'sample_table'

        # Step 1: Create a table (if it doesn't exist)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            age INT
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
        print(f"Table '{table_name}' is ready.")

        # Step 2: Insert a sample record into the table
        insert_query = sql.SQL("""
        INSERT INTO {} (name, age) VALUES (%s, %s)
        """).format(sql.Identifier(table_name))
        cursor.execute(insert_query, ('John Doe', 30))
        connection.commit()
        print("Sample record inserted into the table.")

        # Step 3: Read the record back from the table
        select_query = f"SELECT * FROM {table_name} WHERE name = %s;"
        cursor.execute(select_query, ('John Doe',))
        result = cursor.fetchone()
        
        print("Read from the table:")
        print(f"ID: {result[0]}, Name: {result[1]}, Age: {result[2]}")

        # Clean up: Close the cursor and connection
        cursor.close()
        connection.close()
        print("Connection closed.")

    except Exception as e:
        print(f"An error occurred: {e}")

# Call the function to test the connection
test_neon_connection()
