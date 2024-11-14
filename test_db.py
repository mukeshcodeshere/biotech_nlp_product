import psycopg2
from psycopg2 import sql, OperationalError, ProgrammingError
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Replace with your full connection string
connection_string = "postgresql://db_biotech_owner:t0KZ7pnguiFb@ep-shrill-smoke-a5xu8h2u.us-east-2.aws.neon.tech/db_biotech?sslmode=require"

# Sample function to connect and execute queries
def test_neon_connection():
    connection = None
    cursor = None
    try:
        # Step 1: Try to connect to the database
        logger.debug(f"Attempting to connect to the database using connection string: {connection_string}")
        connection = psycopg2.connect(connection_string)
        cursor = connection.cursor()
        logger.info("Successfully connected to the Neon database.")

        # Step 2: Check connection status
        if connection is None:
            logger.error("Connection could not be established.")
            return
        
        # Sample table name
        table_name = 'sample_table'

        # Step 3: Create a table (if it doesn't exist)
        try:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                age INT
            );
            """
            cursor.execute(create_table_query)
            connection.commit()
            logger.info(f"Table '{table_name}' is ready.")
        except ProgrammingError as e:
            logger.error(f"Error creating table: {e}")
            connection.rollback()
            return

        # Step 4: Insert a sample record into the table
        try:
            insert_query = sql.SQL("""
            INSERT INTO {} (name, age) VALUES (%s, %s)
            """).format(sql.Identifier(table_name))
            cursor.execute(insert_query, ('John Doe', 30))
            connection.commit()
            logger.info("Sample record inserted into the table.")
        except Exception as e:
            logger.error(f"Error inserting record: {e}")
            connection.rollback()
            return

        # Step 5: Read the record back from the table
        try:
            select_query = f"SELECT * FROM {table_name} WHERE name = %s;"
            cursor.execute(select_query, ('John Doe',))
            result = cursor.fetchone()
            if result:
                logger.info("Record read successfully:")
                logger.info(f"ID: {result[0]}, Name: {result[1]}, Age: {result[2]}")
            else:
                logger.warning(f"No record found for name 'John Doe'.")
        except Exception as e:
            logger.error(f"Error reading record: {e}")
            return

    except OperationalError as e:
        logger.error(f"Database connection failed: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        # Clean up: Close the cursor and connection
        if cursor:
            cursor.close()
            logger.debug("Cursor closed.")
        if connection:
            connection.close()
            logger.debug("Connection closed.")

# Call the function to test the connection
test_neon_connection()
