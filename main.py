import sys
from typing import Any, Dict

from src.bigquery.client import BigQueryClient
from src.service.etl_service import ETLService
from src.utils.logger import CloudLogger

logger = CloudLogger(__name__)


def perform_transformations() -> tuple[Dict[str, Any], int]:
    """
    Performs the ETL (Extract, Transform, Load) transformations by initializing the BigQuery client
    and executing the ETL process through the ETLService.

    Returns:
        tuple[Dict[str, Any], int]:
            - A dictionary containing metadata or results from the transformation process.
            - An integer representing the status code (0 for success, 1 for failure).

    Raises:
        Exception: If an unexpected error occurs during the ETL process.
    """
    try:
        logger.info("Initializing bigquery client")
        bq_client = BigQueryClient()
        service = ETLService(client=bq_client)
        service.process_subscription()
        service.process_appointment()
    except Exception as e:
        logger.error("Unexpected error in ETL process", error=e)
        raise


if __name__ == "__main__":
    """
    Entry point of the script. Executes the ETL process and handles errors appropriately.

    Exits:
        - With error code 0 if the ETL process succeeds.
        - With error code 1 if an exception occurs during the process.
    """
    try:
        perform_transformations()
    except Exception as e:
        logger.error("ETL process failed, exiting with error code 1", error=e)
        sys.exit(1)
