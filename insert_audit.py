import logging
import snowflake.connector
import yaml

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Config file containing Snowflake credentials
local_config_file = 'snowflake_credentials.yaml'

def get_config(file_path):
    try:
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logging.error(f"Failed to load configuration from {file_path}: {e}")
        return None

local_config = get_config(local_config_file)

if local_config and 'snowflake' in local_config:
    SNOWFLAKE_CONFIG = {
        'user': local_config['snowflake']['user'],
        'password': local_config['snowflake']['password'],
        'account': local_config['snowflake']['account'],
        'warehouse': local_config['snowflake']['warehouse'],
        'database': local_config['snowflake']['database'],
        'schema': local_config['snowflake']['schema'],
        'role': local_config['snowflake']['role']  # Include role here
    }

    def insert_audit_log(event_type, record_count_before, record_count_after, duration, job_start_time, job_end_time,
                         status, error_message, pipeline_name, source_system, target_system, job_id, alert_triggered, retry_status):
        conn = None
        cursor = None
        try:
            conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            cursor = conn.cursor()

            insert_query = """
            INSERT INTO AUDIT_LOG_PICAFETEAM3 (
                event_type,
                record_count_before,
                record_count_after,
                duration,
                job_start_time,
                job_end_time,
                status,
                error_message,
                pipeline_name,
                source_system,
                target_system,
                job_id,
                alert_triggered,
                retry_status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """

            cursor.execute(insert_query, (
                event_type,
                record_count_before,
                record_count_after,
                duration,
                job_start_time,
                job_end_time,
                status,
                error_message,
                pipeline_name,
                source_system,
                target_system,
                job_id,
                alert_triggered,
                retry_status
            ))

            conn.commit()
            logging.info("Audit log inserted successfully")

        except snowflake.connector.Error as e:
            logging.error(f"Error inserting audit log: {e}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

else:
    logging.error("Failed to retrieve Snowflake configuration from snowflake_credentials.yaml.")

