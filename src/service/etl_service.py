from datetime import datetime, timezone

from ..bigquery.client import BigQueryClient
from ..constants.query_constants import (
    DISTINCT_CLIENT_APPOINTMENT,
    DISTINCT_CLIENT_SUBSCRIPTION,
    T_APPOINTMENT_HELPER,
    T_APPOINTMENT_HELPER_QUERY,
    T_SUBSCRIPTION_HELPER,
    T_SUBSCRIPTION_HELPER_QUERY,
    TRUNCATE_T_APPOINTMENT_HELPER,
    TRUNCATE_T_SUBSCRIPTION_HELPER,
)
from ..transformation.transformations import DataTransformer
from ..utils.logger import CloudLogger


class ETLService:
    """
    A service for executing ETL (Extract, Transform, Load) operations on subscription
    and appointment data.

    Attributes:
        client (BigQueryClient): A BigQuery client instance for database interactions.
        logger (CloudLogger): A logger instance for capturing log messages.
        transformer (DataTransformer): A transformer instance for handling data transformations.
    """

    def __init__(self, client: BigQueryClient):
        """
        Initializes the ETLService class.

        Args:
            client (BigQueryClient): A BigQuery client instance.
        """
        self.client = client
        self.logger = CloudLogger(__name__)
        self.transformer = DataTransformer(client)

    def _process_data(
        self,
        query: str,
        table_name: str,
        transformation_method,
        process_name: str,
        client_query: str,
        truncate_query: str,
    ):
        """
        Generic method to process data with common error handling and logging.

        Args:
            query_constant: Table or query constant
            transformation_method: Transformation method from transformer
            process_name: Name of the process for logging
        """
        start_time = datetime.now(timezone.utc)
        self.logger.info(
            f"Starting data processing for {process_name}",
            timestamp=start_time.isoformat(),
        )
        try:
            clients = self.client.get_client_list(client_query, None, None)
            self.client.delete_data(truncate_query, process_name)
            for clientId in clients:
                self.logger.info(f"Started computing helper transformations for {process_name} and client: {clientId}")
                raw_data = self.client.read_table_data(query, None, clientId, None)

                if raw_data:
                    df = transformation_method(raw_data)
                    self.client.write_to_table(table_name, df)

            self.logger.info(f"Completed data processing for {process_name}")
        except Exception as e:
            self.logger.error(f"Error processing {process_name}: {str(e)}")
            raise

    def process_subscription(self):
        """Process subscription data using generic method."""
        self._process_data(
            T_SUBSCRIPTION_HELPER_QUERY,
            T_SUBSCRIPTION_HELPER,
            self.transformer.subscription_helper_transformation,
            "subscription",
            DISTINCT_CLIENT_SUBSCRIPTION,
            TRUNCATE_T_SUBSCRIPTION_HELPER,
        )

    def process_appointment(self):
        """Process appointment data using generic method."""
        self._process_data(
            T_APPOINTMENT_HELPER_QUERY,
            T_APPOINTMENT_HELPER,
            self.transformer.appointment_helper_transformation,
            "appointment",
            DISTINCT_CLIENT_APPOINTMENT,
            TRUNCATE_T_APPOINTMENT_HELPER,
        )
