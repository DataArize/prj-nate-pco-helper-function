from datetime import datetime, timezone
from typing import Any, Dict

import functions_framework

from src.bigquery.client import BigQueryClient
from src.config.table_config import ETLConfig
from src.constants.environment_constants import SCHEMA_PATH
from src.constants.operation_status import OperationStatus
from src.constants.response import (
    ERROR_MESSAGE,
    STATUS,
    TIMESTAMP,
)
from src.models.processing_result import ProcessingResult
from src.transformations.transformation import DataTransformer
from src.utils.logger import CloudLogger

logger = CloudLogger(__name__)


@functions_framework.http
def perform_transformation(request) -> tuple[Dict[str, Any], int]:
    start_time = datetime.now(timezone.utc)
    logger.info("Starting data processing", timestamp=start_time.isoformat())

    results: Dict[str, ProcessingResult] = {}
    try:
        logger.info("Initializing BigQuery client and data transformer.")
        bq_client = BigQueryClient()
        transformer = DataTransformer(schema_path=SCHEMA_PATH)

        source_tables = ETLConfig.get_source_tables()
        logger.info(f"Found {len(source_tables)} source tables to process.")

        for source_table in source_tables:
            table_start_time = datetime.now(timezone.utc)
            logger.info(f"Starting processing for table: {source_table.table_id}")
            try:
                target_table = ETLConfig.get_target_tables(source_table.table_id)
                logger.info(f"Target table resolved: {target_table.table_id}")



            except Exception as e:
                logger.error(
                    f"Error processing table {source_table.table_id}", error=str(e)
                )
                results[source_table.table_id] = ProcessingResult(
                    records_failed=1,
                    records_processed=0,
                    error_messages=[str(e)],
                    processing_time=(
                        datetime.now(timezone.utc) - table_start_time
                    ).total_seconds(),
                )

    except Exception as e:
        logger.error("Unexpected error in ETL process", error=str(e))
        return {
            STATUS: OperationStatus.FAILURE.value,
            ERROR_MESSAGE: f"ETL process failed: {str(e)}",
            TIMESTAMP: datetime.now(timezone.utc).isoformat(),
        }, 500

if __name__ == '__main__':
    perform_transformation("Hello World!")
