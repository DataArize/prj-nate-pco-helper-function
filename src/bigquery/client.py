from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from google.cloud import bigquery

from ..constants.query_constants import MAX_TIMESTAMP, TIMESTAMP, WHERE_CONDITION
from ..utils.logger import CloudLogger


class BigQueryClient:
    """
    A client class to interact with Google BigQuery. It provides methods to perform operations
    such as retrieving the maximum timestamp from a table, reading table data, writing data to a table,
    and converting Pandas DataFrame columns to BigQuery-compatible data types.
    """

    def __init__(self):
        """
        Initializes the BigQueryClient with a logger and BigQuery client.
        """
        self.logger = CloudLogger(__name__)
        self.client = bigquery.Client()

    def get_max_timestamp(self, table_path: str) -> Optional[datetime]:
        """
        Retrieves the maximum timestamp from a specified BigQuery table.

        Args:
            table_path (str): The full path of the BigQuery table.

        Returns:
            Optional[datetime]: The maximum timestamp if found, otherwise None.
        """
        try:
            query = f"SELECT MAX(recordCreatedAt) as max_timestamp FROM `{table_path}`"
            self.logger.info(f"Executing query to get max timestamp: {query}")

            query_job = self.client.query(query)
            result = [dict(row.items()) for row in query_job]

            if result and result[0].get("max_timestamp") is not None:
                max_timestamp = result[0]["max_timestamp"]
                self.logger.info(
                    f"Max timestamp for table {table_path}: {max_timestamp}"
                )
                return max_timestamp
            else:
                self.logger.warning(
                    f"No data found or max timestamp is NULL for table {table_path}"
                )
                return None

        except Exception as e:
            self.logger.error(
                f"Failed to get max timestamp for table {table_path}. Error: {str(e)}"
            )
            raise

    def read_table_data(
        self, query: str, max_timestamp: datetime, batch_size: int = None
    ) -> List[Dict[str, Any]]:
        """
        Reads data from a BigQuery table based on the provided SQL query, with optional parameters
        for filtering by maximum timestamp and limiting the number of rows.

        Args:
            query (str): The SQL query to execute.
            max_timestamp (datetime): The maximum timestamp to filter the data.
            batch_size (int, optional): The number of rows to limit the query to. Defaults to None.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the query results.
        """
        try:
            if max_timestamp:
                query += WHERE_CONDITION
            if batch_size:
                query += f" LIMIT {batch_size}"

            self.logger.info(f"Executing query: {query}")
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        MAX_TIMESTAMP, TIMESTAMP, max_timestamp
                    )
                ]
            )
            query_job = self.client.query(query, job_config=job_config)
            data = [dict(row.items()) for row in query_job]

            self.logger.info(f"Query executed successfully. Fetched {len(data)}")
            return data
        except Exception as e:
            self.logger.error(f"Failed to read data for query {query}. Error: {str(e)}")
            raise

    def write_to_table(self, table_path: str, data: pd.DataFrame) -> None:
        """
        Writes data from a Pandas DataFrame to a specified BigQuery table.

        Args:
            table_path (str): The full path of the BigQuery table.
            data (pd.DataFrame): The Pandas DataFrame containing the data to write.

        Returns:
            None
        """
        try:
            self.logger.info(f"Inserting data into {table_path}")
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )

            self.logger.info("Initializing BigQuery load job")
            job = self.client.load_table_from_dataframe(data, table_path, job_config)

            job.result()

            self.logger.info(f"Data written successfuly into table: {table_path}")
            return
        except Exception:
            raise

    def convert_to_bigquery_dtype(self, df, column_dtype_map):
        """
        Converts the data types of specified columns in a Pandas DataFrame to BigQuery-compatible data types.

        Args:
            df (pd.DataFrame): The Pandas DataFrame to convert.
            column_dtype_map (Dict[str, str]): A mapping of column names to BigQuery data types (e.g., 'STRING', 'INTEGER').

        Returns:
            pd.DataFrame: The DataFrame with converted data types.
        """
        for column, bq_type in column_dtype_map.items():
            if column not in df.columns:
                continue

            if bq_type == "STRING":
                df[column] = df[column].astype(str)
            elif bq_type == "INTEGER":
                df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
            elif bq_type == "FLOAT":
                df[column] = pd.to_numeric(df[column], errors="coerce")
            elif bq_type == "BOOLEAN":
                df[column] = df[column].astype(bool)
            elif bq_type == "DATETIME":
                df[column] = pd.to_datetime(df[column], errors="coerce")

        return df
