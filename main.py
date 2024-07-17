import logging
from log_historical_data_transfer import log_historical_data_transfer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    try:
        log_historical_data_transfer()
    except Exception as e:
        logging.error(f"Error during data transfer: {e}")

if __name__ == "__main__":
    main()

