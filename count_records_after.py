import logging
import yaml
import snowflake.connector


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_snowflake_credentials_and_tables(config_file):
    try:
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        if 'snowflake' in config:
            local_config = config['snowflake']
            account = local_config['account']
            user = local_config['user']
            password = local_config['password']
            database = local_config['database']
            schema = local_config['schema']
            warehouse = local_config['warehouse']
            role = local_config['role']
            tables = local_config['tables']
            return account, user, password, database, schema, warehouse, role, tables
        else:
            logging.error("Snowflake configuration missing in the YAML file.")
            return None
    except Exception as e:
        logging.error(f"Failed to load Snowflake configuration from {config_file}: {e}")
        return None


def table_exists(conn, database, schema, table):
    query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table}'"
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        return result[0] > 0
    except snowflake.connector.Error as e:
        logging.error(f"Error checking existence of table '{database}.{schema}.{table}': {e}")
        return False


def count_records_snowflake(config_file):
    snowflake_credentials = get_snowflake_credentials_and_tables(config_file)
    if not snowflake_credentials:
        return 0

    account, user, password, database, schema, warehouse, role, tables = snowflake_credentials

    conn = None
    try:
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse,
            role=role
        )
        logging.info("Successfully connected to Snowflake.")

        record_counts = []

        for table in tables:
            if not table_exists(conn, database, schema, table):
                logging.error(f"Table '{schema}.{table}' does not exist or user does not have access.")
                continue

            try:
                query = f'SELECT COUNT(*) AS cnt FROM "{database}"."{schema}"."{table}"'
                cursor = conn.cursor()
                cursor.execute(query)
                record_count = cursor.fetchone()[0]
                record_counts.append((table, record_count))
                cursor.close()
            except snowflake.connector.Error as e:
                logging.error(f"Error executing count query for table '{table}': {e}")

        total_record_count = sum(count for _, count in record_counts)
        logging.info(f"Total record count in Snowflake database: {total_record_count}")

        return total_record_count

    except snowflake.connector.Error as e:
        logging.error(f"Error connecting to Snowflake or counting records: {e}")
        return 0

    finally:
        if conn:
            conn.close()
            logging.info("Snowflake connection closed.")

