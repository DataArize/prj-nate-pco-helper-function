import os
from dataclasses import dataclass
from typing import List

from google.cloud import bigquery

from ..constants.environment_constants import (
    BQ_DATASET_ID,
    BQ_TRANSFORMATION_LAYER,
    CUSTOMER_HISTORY_VIEW_TABLE,
    GCP_PROJECT_ID,
    SCHEMA_PATH,
)
from ..constants.query_constants import CUSTOMER_HISTORY_VIEW
from ..utils.logger import CloudLogger

logger = CloudLogger(__name__)


@dataclass(frozen=True)
class TableConfig:
    project_id: str
    dataset_id: str
    transformation_layer: str
    table_id: str

    def __post_init__(self) -> None:
        assert self.project_id, "Project ID cannot be empty"
        assert self.dataset_id, "Dataset ID cannot be empty"
        assert self.table_id, "Table ID cannot be empty"
        assert (
            self.transformation_layer
        ), "Transformation layer (Dataset) cannot be empty"

    @property
    def fetch_query_to_execute(self) -> str:
        logger.info(f"Fetching query for table_id: {self.table_id}")
        if self.table_id == CUSTOMER_HISTORY_VIEW_TABLE:
            return CUSTOMER_HISTORY_VIEW

    @property
    def full_transformation_table_path(self) -> str:
        full_path = f"{self.project_id}.{self.transformation_layer}.{self.table_id}"
        logger.info(f"Full transformation table path: {full_path}")
        return full_path


class ETLConfig:
    PROJECT_ID = os.getenv(GCP_PROJECT_ID)
    DATASET_ID = os.getenv(BQ_DATASET_ID)
    TRANSFORMATION_LAYER = os.getenv(BQ_TRANSFORMATION_LAYER)

    if not PROJECT_ID:
        logger.error(f"{GCP_PROJECT_ID} environment variable is not set")
        raise ValueError(f"{GCP_PROJECT_ID} environment variable is not set")

    TABLE_MAPPINGS = {
        os.getenv(CUSTOMER_HISTORY_VIEW_TABLE): os.getenv(CUSTOMER_HISTORY_VIEW_TABLE)
    }

    @classmethod
    def get_source_tables(cls) -> List[TableConfig]:
        logger.info("Fetching source tables from TABLE_MAPPINGS")
        source_tables = [
            TableConfig(
                project_id=cls.PROJECT_ID,
                dataset_id=cls.DATASET_ID,
                transformation_layer=cls.TRANSFORMATION_LAYER,
                table_id=source_table,
            )
            for source_table in cls.TABLE_MAPPINGS.keys()
        ]
        logger.info(f"Source tables fetched: {source_tables}")
        return source_tables

    @classmethod
    def get_target_tables(cls, source_table: str) -> TableConfig:
        logger.info(f"Fetching target table for source_table: {source_table}")
        target_table = cls.TABLE_MAPPINGS.get(source_table)
        if not target_table:
            logger.error(f"No target table mapping found for {source_table}")
            raise ValueError(f"No target table mapping found for {source_table}")

        table_config = TableConfig(
            table_id=target_table,
            project_id=cls.PROJECT_ID,
            dataset_id=cls.DATASET_ID,
            transformation_layer=cls.TRANSFORMATION_LAYER,
        )
        logger.info(f"Target table config: {table_config}")
        return table_config

