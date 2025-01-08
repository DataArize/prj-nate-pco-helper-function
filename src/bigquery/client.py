from datetime import date, datetime
from typing import Any, Dict, List

import pandas as pd
from google.cloud import bigquery

from ..config.table_config import ETLConfig, TableConfig
from ..constants.operation_status import OperationStatus
from ..models.result import Result
from ..utils.logger import CloudLogger


class BigQueryClient:
    def __init__(self):
        self.logger = CloudLogger(__name__)
        self.client = bigquery.Client()

    def read_table_data(
        self, table_config: TableConfig, batch_size: int = None
    ) -> Result[List[Dict[str, Any]]]:
        try:
            query = table_config.fetch_query_to_execute
            if batch_size:
                query += f" LIMIT {batch_size}"

            self.logger.info(f"Executing query: {query}")
            query_job = self.client.query(query)
            data = [dict(row.items()) for row in query_job]

            self.logger.info(
                f"Query executed successfully. Fetched {len(data)} rows for table: {table_config.table_id}"
            )
            return Result(status=OperationStatus.SUCCESS, data=data)
        except Exception as e:
            self.logger.error(
                f"Failed to read data for table {table_config.table_id}. Error: {str(e)}"
            )
            return Result(
                status=OperationStatus.FAILURE,
                error=f"Failed to read Query: {table_config.fetch_query_to_execute}: {str(e)}",
            )

    def serialize_for_bigquery(self, obj: Any) -> Any:
        """
        Serialize objects for BigQuery, handling special cases like timestamps.
        """
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        elif pd.isna(obj):
            return None
        return obj

    def prepare_data_for_bigquery(
        self, data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Prepare data for BigQuery by converting all values to JSON-serializable format.
        """
        self.logger.info("Preparing data for BigQuery serialization")
        serialized_data = []
        for record in data:
            serialized_record = {
                key: self.serialize_for_bigquery(value) for key, value in record.items()
            }
            serialized_data.append(serialized_record)
        self.logger.info(
            f"Serialization complete. Prepared {len(serialized_data)} records."
        )
        return serialized_data

    def write_to_table(
        self, table_config: TableConfig, data: List[Dict[str, Any]]
    ) -> Result[None]:
        try:
            self.logger.info(f"Fetching schema for table: {table_config.table_id}")
            schema = ETLConfig.get_bigquery_schema(table_id=table_config.table_id)

            self.logger.info(
                f"Preparing to write {len(data)} records to table: {table_config.full_transformation_table_path}"
            )
            serialized_data = self.prepare_data_for_bigquery(data)

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
                schema=schema,
            )

            self.logger.info("Initiating BigQuery load job")
            job = self.client.load_table_from_json(
                serialized_data,
                table_config.full_transformation_table_path,
                job_config=job_config,
            )

            job.result()

            self.logger.info(
                f"Data successfully written to table: {table_config.table_id}"
            )
            return Result(status=OperationStatus.SUCCESS)

        except Exception as e:
            self.logger.error(
                f"Failed to write data to table {table_config.table_id}. Error: {str(e)}"
            )
            return Result(
                status=OperationStatus.FAILURE.value,
                error=f"Failed to write data to {table_config.table_id}: {str(e)}",
            )
