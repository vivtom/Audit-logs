import logging
import psycopg2
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def record_count_before(config_file):

    try:
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
    except Exception as e:
        logging.error(f"Failed to load configuration from {config_file}: {e}")
        return 0

    if config and 'postgres' in config:
        POSTGRES_CONFIG = {
            'dbname': config['postgres']['dbname'],
            'user': config['postgres']['user'],
            'password': config['postgres']['password'],
            'host': config['postgres']['host'],
            'port': config['postgres']['port']
        }
        table_names = config['postgres']['postgres_table_names']
    else:
        logging.error("Failed to retrieve PostgreSQL configuration from config.yml")
        return 0

    conn = None
    cursor = None
    total_record_count = 0

    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        if conn.status == psycopg2.extensions.STATUS_READY:
            logging.info("Successfully connected to PostgreSQL.")
        else:
            logging.warning("Connection to PostgreSQL established, but status is not ready.")

        cursor = conn.cursor()

        # Construct and execute the query to fetch counts from multiple tables
        for table in table_names:
            logging.info(f"Counting records in table: {table}")
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            total_record_count += count
            logging.info(f"Count for table {table}: {count}")

        logging.info(f"Total record count: {total_record_count}")
        return total_record_count

    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL or counting records: {e}")
        return 0

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logging.info("PostgreSQL connection closed.")
