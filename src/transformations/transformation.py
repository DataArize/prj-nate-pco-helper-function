from typing import Any, Dict, List

import pandas as pd

from ..bigquery.client import BigQueryClient
from ..constants.operation_status import OperationStatus
from ..models.result import Result
from ..utils.logger import CloudLogger


class DataTransformer:
    def __init__(self, schema_path: str):
        self.logger = CloudLogger(__name__)
        self.client = BigQueryClient()

    def transform_dataframe(self, df: pd.DataFrame, table_id: str) -> pd.DataFrame:
        self.logger.info(f"Starting transformation for table '{table_id}'")

        try:
            transformed_df = df.copy()
            self.logger.info(f"Completed transformation for table '{table_id}'")

            return transformed_df
        except Exception as e:
            self.logger.error(f"Error transforming table '{table_id}': {str(e)}")
            raise

    def transform_batch(
        self, data: List[Dict[str, Any]], batch_size: int, table_id: str
    ) -> Result[List[Dict[str, Any]]]:
        self.logger.info(
            f"Starting batch transformation for table '{table_id}' with batch size {batch_size}"
        )

        try:
            df = pd.DataFrame(data)
            assert not df.empty, "Empty DataFrame received"

            transformed_df = self.transform_dataframe(df, table_id)

            self.logger.info(f"Batch transformation successful for table '{table_id}'")
            return Result(
                status=OperationStatus.SUCCESS, data=transformed_df.to_dict("records")
            )

        except Exception as e:
            self.logger.error(
                f"Batch transformation failed for table '{table_id}': {str(e)}"
            )
            return Result(
                status=OperationStatus.FAILURE.value,
                error=f"Transformation failed: {str(e)}",
            )
