from datetime import time
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from ..bigquery.client import BigQueryClient
from ..constants.dataframe_constants import (
    APPOINTMENT_DATE,
    AVERAGE_MINUTES,
    CONSTAINED_TIME,
    CRM_MINUTES,
    DRIVE_TIME,
    DURATION,
    FILL_IN_ERRORS,
    IS_ERROR,
    MASTER_ACCOUNT_ID,
    MINUTES_OUTLIER_OUT,
    MULTIVISIT_COUNT,
    MULTIVISIT_CRM_TIME,
    MULTIVIST,
    MULTIVIST_ADJUSTED_MINUTES,
    MULTIVIST_DURATION,
    PREFERRED_DAYS,
    PREFERRED_END,
    PREFERRED_START,
    STATUS,
    TIME_IN,
    TIME_OUT,
    TOTAL_TIME,
    VALUE,
)
from ..utils.data_validation import DataValidation
from ..utils.logger import CloudLogger
from .appointment_schema import column_types


class DataTransformer:
    """
    A class for transforming data used in subscription and appointment processing.

    Attributes:
        client (BigQueryClient): A BigQuery client instance for handling database interactions.
        logger (CloudLogger): A logger instance for capturing log messages.
        validator (DataValidation): A data validation utility for ensuring data integrity.
    """

    def __init__(self, client: BigQueryClient):
        """
        Initializes the DataTransformer class.

        Args:
            client (BigQueryClient): A BigQuery client instance.
        """
        self.client = client
        self.logger = CloudLogger(__name__)
        self.validator = DataValidation()

    def subscription_helper_transformation(
        self, data: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        Transforms subscription-related data into a structured DataFrame.

        Args:
            data (List[Dict[str, Any]]): A list of dictionaries representing subscription data.

        Returns:
            pd.DataFrame: A transformed DataFrame with constrained time information.

        Raises:
            Exception: If transformation fails due to invalid data or processing errors.
        """
        required_columns = [PREFERRED_DAYS, PREFERRED_START, PREFERRED_END]
        type_checks = {
            PREFERRED_DAYS: [int, np.integer],
            PREFERRED_START: [time],
            PREFERRED_END: [time],
        }

        df = self.validator.validate_dataframe(data, required_columns, type_checks)

        try:
            df[CONSTAINED_TIME] = (
                (df[PREFERRED_DAYS] > 0)
                & (df[PREFERRED_START] > time(0, 0, 0))
                & (df[PREFERRED_END] > time(0, 0, 0))
            )

            df.drop(
                columns=[PREFERRED_DAYS, PREFERRED_START, PREFERRED_END], inplace=True
            )

            return df
        except Exception as e:
            self.logger.error(f"subscription transformation failed: {str(e)}")
            raise

    def appointment_helper_transformation(
        self, data: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        Transforms appointment-related data into a structured DataFrame.

        Args:
            data (List[Dict[str, Any]]): A list of dictionaries representing appointment data.

        Returns:
            pd.DataFrame: A transformed DataFrame with multivisit and timing information.

        Raises:
            Exception: If transformation fails due to invalid data or processing errors.
        """
        required_columns = [
            MASTER_ACCOUNT_ID,
            APPOINTMENT_DATE,
            STATUS,
            CRM_MINUTES,
            DURATION,
            VALUE,
            AVERAGE_MINUTES,
        ]

        type_checks = {
            MASTER_ACCOUNT_ID: [int, np.integer],
            STATUS: [int, np.integer],
            CRM_MINUTES: [float, np.floating],
            DURATION: [float, np.floating],
        }

        df = self.validator.validate_dataframe(data, required_columns, type_checks)

        try:
            # Multivist condition calculation
            multivisit_condition = (
                df.groupby([MASTER_ACCOUNT_ID, APPOINTMENT_DATE])
                .apply(lambda group: (group[STATUS] == 1).sum() > 1)
                .reset_index(name=MULTIVIST)
            )

            # Merge and transformations
            df = df.merge(
                multivisit_condition,
                on=[MASTER_ACCOUNT_ID, APPOINTMENT_DATE],
                how="left",
            )

            df[IS_ERROR] = df[CRM_MINUTES] == 0.0
            df[MINUTES_OUTLIER_OUT] = df.apply(self.compute_minutes_outlier_out, axis=1)
            df[FILL_IN_ERRORS] = df.apply(self.compute_fill_in_errors, axis=1)

            # Filtered and grouped calculations
            df_filtered = df[df[STATUS] == 1]
            df[MULTIVIST_DURATION] = df_filtered.groupby(
                [MASTER_ACCOUNT_ID, APPOINTMENT_DATE]
            )[DURATION].transform("sum")
            # Compute multivist CRM time
            df = self.compute_multivist_crm_time(df)

            # Multivist count calculation
            multivisit_count_df = (
                df.groupby([MASTER_ACCOUNT_ID, APPOINTMENT_DATE])
                .apply(lambda group: (group[STATUS] == 1).sum())
                .reset_index(name=MULTIVISIT_COUNT)
            )

            # Merge and numeric conversions
            df = df.merge(
                multivisit_count_df,
                on=[MASTER_ACCOUNT_ID, APPOINTMENT_DATE],
                how="left",
            )

            # Safe numeric conversions with error handling
            numeric_columns = [
                MULTIVISIT_CRM_TIME,
                MULTIVISIT_COUNT,
                VALUE,
                FILL_IN_ERRORS,
                MINUTES_OUTLIER_OUT,
            ]
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            # Compute derived columns
            df[MULTIVIST_ADJUSTED_MINUTES] = np.where(
                df[MULTIVIST],
                df[MULTIVISIT_CRM_TIME] / df[MULTIVISIT_COUNT],
                df[FILL_IN_ERRORS],
            )

            df[DRIVE_TIME] = df[VALUE] / df[MULTIVISIT_COUNT].replace(0, np.nan)
            df[TOTAL_TIME] = df[DRIVE_TIME].fillna(0) + df[
                MULTIVIST_ADJUSTED_MINUTES
            ].fillna(0)

            df.drop(
                columns=[
                    MASTER_ACCOUNT_ID,
                    APPOINTMENT_DATE,
                    TIME_IN,
                    TIME_OUT,
                    STATUS,
                    DURATION,
                    VALUE,
                    AVERAGE_MINUTES,
                ],
                inplace=True,
            )

            df = self.client.convert_to_bigquery_dtype(df, column_types)

            return df
        except Exception as e:
            self.logger.error(f"Appointment transformation failed: {str(e)}")
            raise

    def compute_minutes_outlier_out(self, row: pd.Series) -> float:
        """
        Computes the outlier-adjusted CRM minutes for a given row.

        Args:
            row (pd.Series): A row of data containing status, duration, and CRM minutes.

        Returns:
            float: The outlier-adjusted CRM minutes.
        """
        if row[STATUS] == 1:
            return max(min(row[DURATION] * 2, row[CRM_MINUTES]), row[DURATION] * 0.25)
        else:
            return 0.0

    def compute_fill_in_errors(self, row: pd.Series) -> float:
        """
        Computes the error-adjusted CRM minutes for a given row.

        Args:
            row (pd.Series): A row of data containing error status and average minutes.

        Returns:
            float: The error-adjusted CRM minutes.
        """
        if row[IS_ERROR]:
            return row[AVERAGE_MINUTES]
        else:
            return row[MINUTES_OUTLIER_OUT]

    def compute_multivist_crm_time(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Computes the total CRM time for multivisit appointments.

        Args:
            df (pd.DataFrame): The DataFrame containing appointment data.

        Returns:
            pd.DataFrame: The updated DataFrame with multivisit CRM time calculated.
        """
        max_times = (
            df[df[STATUS] == 1]
            .groupby([MASTER_ACCOUNT_ID, APPOINTMENT_DATE])[TIME_OUT]
            .max()
        )

        min_times = (
            df[df[STATUS] == 1]
            .groupby([MASTER_ACCOUNT_ID, APPOINTMENT_DATE])[TIME_IN]
            .min()
        )

        time_diff = (max_times - min_times).dt.total_seconds() / 60

        def calculate_value(row):
            if row[MULTIVIST] and row[STATUS] == 1:
                min(
                    row[DURATION] * 0.25,
                    max(
                        2 * row[DURATION],
                        time_diff.get(
                            (row[MASTER_ACCOUNT_ID], row[APPOINTMENT_DATE]), 0
                        ),
                    ),
                )
            return 0.0

        df[MULTIVISIT_CRM_TIME] = df.apply(calculate_value, axis=1)

        return df
