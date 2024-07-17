from datetime import datetime
import logging
import yaml
from count_records_before import record_count_before
from count_records_after import count_records_snowflake
from insert_audit import insert_audit_log
from generate_job_id import generate_job_id


pipeline_name = 'historical'
source_system = 'ec2_postgres'
target_system = 'snowflake'
event_type = 'insert'


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def log_historical_data_transfer():
    config_file = 'config.yml'
    local_config_file = 'snowflake_credentials.yaml'


    record_count_before_value = record_count_before(config_file)

    job_start_time = datetime.now()
    job_id = generate_job_id()

    try:
        # Simulate the data transfer process here or replace with actual data transfer logic
        # For example:
        # perform_data_transfer()

        # Fetch table names from config
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        table_names = config['postgres']['postgres_table_names']

        # Fetch record count after transfer
        record_count_after_value = count_records_snowflake(local_config_file)

        job_end_time = datetime.now()
        duration = (job_end_time - job_start_time).total_seconds()
        status = 'success'
        error_message = ''
        alert_triggered = False
        retry_status = False

        insert_audit_log(
            event_type, record_count_before_value, record_count_after_value, duration, job_start_time, job_end_time,
            status, error_message, pipeline_name, source_system,
            target_system, job_id, alert_triggered, retry_status
        )

    except Exception as e:
        job_end_time = datetime.now()
        duration = (job_end_time - job_start_time).total_seconds()
        logging.error(f"Error during data transfer: {e}")
        status = 'fail'
        error_message = 'Can not transfer data'
        alert_triggered = True
        retry_status = True
        record_count_after_value = 0

        insert_audit_log(
            event_type, record_count_before_value, record_count_after_value, duration, job_start_time, job_end_time,
            status, error_message, pipeline_name, source_system,
            target_system, job_id, alert_triggered, retry_status
        )


if __name__ == "__main__":
    log_historical_data_transfer()
