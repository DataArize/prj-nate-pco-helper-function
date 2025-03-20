from datetime import time
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import ast

from ..bigquery.client import BigQueryClient
from ..constants.dataframe_constants import (
    ANNUAL_RECURRING_SERVICES,
    ANNUAL_RECURRING_VALUE,
    APPOINTMENT_DATE,
    CLIENT_ID,
    COMPUTED_APPOINTMENT_DATE,
    CONSTAINED_TIME,
    CRM_MINUTES,
    CRM_SOURCE,
    DURATION,
    DURATION_RATIO,
    DURATION_RATIO_AVG,
    DURATION_RATIO_INITIALS,
    DURATION_RATIO_OVERALL,
    ERRORS_OUT,
    IN_REF_PERIOD,
    IN_TWO_YEAR_LOOK_BACK,
    INCLUDE_IN_AVERAGE,
    IS_ERROR,
    IS_INITIAL,
    MASTER_ACCOUNT_ID,
    MINUTES_OUTLIER_OUT,
    MULTIVISIT_COUNT,
    MULTIVISIT_CRM_TIME,
    MULTIVISIT_END_DATE,
    MULTIVISIT_START_DATE,
    MULTIVIST,
    ONSITE_MINUTES,
    OUTLIER_CEIL_LONG,
    OUTLIER_CEIL_SHORT,
    OUTLIER_FLOOR,
    OUTLIERS_OUT,
    PREFERRED_DAYS,
    PREFERRED_END,
    PREFERRED_START,
    STATUS,
    TIME_IN,
    TIME_OUT,
    TYPE, IS_ACTIVE, END_AFTER_REF_DATE, START_ON_OR_BEFORE_REF_DATE, INDIVIDUAL_ACCOUNT_ID, SERVICED_BY,
    ZERO_VISIT_TIME, RECURRING_TICKET, TICKET_ID,
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
            df[IS_ACTIVE] = df[END_AFTER_REF_DATE] & df[START_ON_OR_BEFORE_REF_DATE]

            df[CONSTAINED_TIME] = df[ANNUAL_RECURRING_SERVICES].where(
                (df[PREFERRED_DAYS] > 0)
                | (df[PREFERRED_START] > time(0, 0, 0))
                | (df[PREFERRED_END] > time(0, 0, 0)),
                0,
            )

            # Function to extract ticketID safely with debugging
            def extract_ticket_id(row):
                try:
                    parsed_dict = ast.literal_eval(row)  # Convert string to dictionary
                    if isinstance(parsed_dict, dict):  # Ensure it's a dictionary
                        ticket_id = parsed_dict.get('ticketID', 'None')
                        if ticket_id == "None":
                            print(f"Warning: 'ticketID' missing in row: {row}")  # Debug missing key
                        return ticket_id
                    else:
                        print(f"Unexpected format (not a dict): {row}")  # Debug non-dict cases
                        return "None"
                except (SyntaxError, ValueError) as e:
                    print(f"Error parsing row: {row} | Error: {e}")  # Debug syntax errors
                    return "None"


            df[TICKET_ID] = df[RECURRING_TICKET].apply(extract_ticket_id)

            df.drop(
                columns=[
                    PREFERRED_DAYS,
                    PREFERRED_START,
                    PREFERRED_END,
                    ANNUAL_RECURRING_VALUE,
                    ANNUAL_RECURRING_SERVICES,
                    RECURRING_TICKET,
                ],
                inplace=True,
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
            # VALUE,
            # AVERAGE_MINUTES,
        ]

        type_checks = {
            MASTER_ACCOUNT_ID: [int, np.floating],
            STATUS: [int, np.integer],
            CRM_MINUTES: [float, np.floating],
            DURATION: [float, np.floating],
        }

        df = self.validator.validate_dataframe(
            data, required_columns, "appointment", None
        )

        try:
            #QUICK_FIX: CRM_MINUTES fill null values as zero
            df[TIME_IN] = df[TIME_IN].fillna(0)
            df[TIME_OUT] = df[TIME_OUT].fillna(0)
            df[CRM_MINUTES] = df[CRM_MINUTES].fillna(0)
            df[DURATION_RATIO] = df[CRM_MINUTES] / df[DURATION]
            df[IS_ERROR] = (df[CRM_MINUTES] <= 1.0) | (df[TIME_IN] == 0) | (df[TIME_OUT] == 0)

            df[OUTLIER_FLOOR] = df[CRM_MINUTES] < 5.0
            df[OUTLIER_CEIL_SHORT] = np.where(
                df[DURATION] < 45, df[CRM_MINUTES] > 60, False
            )
            df[OUTLIER_CEIL_LONG] = np.where(
                df[DURATION] < 45, False, df[CRM_MINUTES] > 2 * df[DURATION]
            )
            df[IN_TWO_YEAR_LOOK_BACK] = df[IN_REF_PERIOD]
            df[INCLUDE_IN_AVERAGE] = ~df[
                [
                    IS_ERROR,
                    OUTLIER_FLOOR,
                    OUTLIER_CEIL_LONG,
                    OUTLIER_CEIL_SHORT,
                    IN_TWO_YEAR_LOOK_BACK,
                ]
            ].any(axis=1)

            mask = df[INCLUDE_IN_AVERAGE]

            df.loc[mask, DURATION_RATIO_OVERALL] = (
                df.loc[mask]
                .groupby([TYPE, CLIENT_ID, CRM_SOURCE])[DURATION_RATIO]
                .transform("mean")
            )

            df.loc[~mask, DURATION_RATIO_OVERALL] = np.nan

            mask = (df[INCLUDE_IN_AVERAGE]) & (df[IS_INITIAL] == 1)

            df.loc[mask, DURATION_RATIO_INITIALS] = (
                df.loc[mask]
                .groupby([TYPE, CLIENT_ID, CRM_SOURCE])[DURATION_RATIO]
                .transform("mean")
            )

            df.loc[~mask, DURATION_RATIO_INITIALS] = np.nan

            df = df.groupby([TYPE, CLIENT_ID, CRM_SOURCE], group_keys=False).apply(
                self.assign_duration_ratio_avg
            )

            df[ERRORS_OUT] = np.where(
                df[IS_ERROR], df[DURATION] * df[DURATION_RATIO_AVG], df[CRM_MINUTES]
            )

            df[OUTLIERS_OUT] = np.where(
                df[IS_ERROR],
                df[ERRORS_OUT],
                np.where(
                    df[OUTLIER_FLOOR],  # If OUTLIER_FLOOR (AA2) is True...
                    5,  # Return 5
                    np.where(
                        df[
                            OUTLIER_CEIL_SHORT
                        ],  # If OUTLIER_CEIL_SHORT (AB2) is True...
                        60,  # Return 60
                        np.where(
                            df[
                                OUTLIER_CEIL_LONG
                            ],  # If OUTLIER_CEIL_LONG (AC2) is True...
                            2 * df[DURATION],  # Return 2 * DURATION
                            df[ERRORS_OUT],  # Else, return ERRORS_OUT
                        ),
                    ),
                ),
            )
            df["multi_visit_key"] = df[INDIVIDUAL_ACCOUNT_ID].astype(str) + df[SERVICED_BY].astype(str) + df[
                COMPUTED_APPOINTMENT_DATE].astype(str) + df[ZERO_VISIT_TIME].astype(str)

            mask = (df[ZERO_VISIT_TIME] == False) & (df[STATUS] == 1)
            df[MULTIVISIT_COUNT] = df["multi_visit_key"].map(
                df.loc[mask, "multi_visit_key"].value_counts()
            ).fillna(0)


            # df[MULTIVISIT_COUNT] = df["multi_visit_key"].map(
            #     df["multi_visit_key"].value_counts())

            df.drop(columns=["multi_visit_key"], inplace=True)
            df.drop(columns=[INDIVIDUAL_ACCOUNT_ID], inplace=True)
            df.drop(columns=[SERVICED_BY], inplace=True)

            df[TIME_IN] = pd.to_datetime(df[TIME_IN], utc=True)
            df[TIME_OUT] = pd.to_datetime(df[TIME_OUT], utc=True)

            # df_for_agg = df[df[ZERO_VISIT_TIME] == False and df[STATUS] == 1]
            df_for_agg = df[(df[ZERO_VISIT_TIME] == False) & (df[STATUS] == 1)]

            default_timestamp = pd.Timestamp("1970-01-01 00:00:00", tz="UTC")

            appointment_range_df = (
                df_for_agg.groupby(
                    [
                        MASTER_ACCOUNT_ID,
                        CLIENT_ID,
                        CRM_SOURCE,
                        COMPUTED_APPOINTMENT_DATE,
                    ]
                )
                .agg(
                    # multivisitStartDate=(TIME_IN, "min"),
                    multivisitStartDate=(
                        TIME_IN,
                        lambda x: x[x != default_timestamp].min()  # only consider values that are not the default
                    ),
                    # multivisitEndDate=(TIME_OUT, "max"),
                    multivisitEndDate=(
                        TIME_OUT,
                        lambda x: x[x != default_timestamp].max()  # only consider values that are not the default
                    ),
                    multivisitCount=(MULTIVISIT_COUNT, "max"),
                )
                .reset_index()
            )

            appointment_range_df.loc[
                appointment_range_df[MULTIVISIT_COUNT] == 1,
                [MULTIVISIT_START_DATE, MULTIVISIT_END_DATE],
            ] = pd.NaT

            df = df.merge(
                appointment_range_df[
                    [
                        MASTER_ACCOUNT_ID,
                        CLIENT_ID,
                        CRM_SOURCE,
                        COMPUTED_APPOINTMENT_DATE,
                        MULTIVISIT_START_DATE,
                        MULTIVISIT_END_DATE,
                    ]
                ],
                on=[
                    MASTER_ACCOUNT_ID,
                    CLIENT_ID,
                    CRM_SOURCE,
                    COMPUTED_APPOINTMENT_DATE,
                ],
                how="left",
            )

            df[MULTIVISIT_START_DATE] = pd.to_datetime(
                df[MULTIVISIT_START_DATE], errors="coerce"
            )
            df[MULTIVISIT_END_DATE] = pd.to_datetime(
                df[MULTIVISIT_END_DATE], errors="coerce"
            )

            # First compute numeric values (using np.nan for rows not in reference period)
            onsite_numeric = np.where(
                df[ZERO_VISIT_TIME],
                0,
                np.where(
                    df[IN_REF_PERIOD],
                    np.where(
                        df[MULTIVISIT_COUNT] < 2,
                        df[OUTLIERS_OUT],
                        np.where(
                            df[MULTIVISIT_START_DATE].notnull() & df[MULTIVISIT_END_DATE].notnull(),
                            (((df[MULTIVISIT_END_DATE] - df[MULTIVISIT_START_DATE])
                              / pd.Timedelta(days=1)) * 1440) / df[MULTIVISIT_COUNT],
                            np.nan,
                        ),
                    ),
                    np.nan  # using np.nan instead of the string for now
                )
            )

            # Assign the result to the column as object dtype so it can later hold strings
            df[ONSITE_MINUTES] = onsite_numeric.astype(object)

            # Now, for rows not in the reference period, replace the value with the desired string
            df.loc[~df[IN_REF_PERIOD], ONSITE_MINUTES] = "Not in Ref Period"
            df[ONSITE_MINUTES] = df[ONSITE_MINUTES].fillna(0)
            # df[MULTIVISIT_START_DATE] = df[MULTIVISIT_START_DATE].fillna(0)
            # df[MULTIVISIT_END_DATE] = df[MULTIVISIT_END_DATE].fillna(0)


            df = df.drop(
                columns=[
                    TIME_IN,
                    TIME_OUT,
                    APPOINTMENT_DATE,
                    TYPE,
                    IS_INITIAL,
                    STATUS,
                    DURATION,
                ]
            )
            df = self.client.convert_to_bigquery_dtype(df, column_types)

            return df
        except Exception as e:
            self.logger.error(f"Appointment transformation failed: {str(e)}")
            raise

    def assign_duration_ratio_avg(self, group):
        # Try to get a non-null overall value from the group.
        overall_vals = group[DURATION_RATIO_OVERALL].dropna()
        if not overall_vals.empty:
            value = overall_vals.iloc[0]
        else:
            # Fall back to initials if overall is missing.
            initials_vals = group[DURATION_RATIO_INITIALS].dropna()
            if not initials_vals.empty:
                value = initials_vals.iloc[0]
            else:
                value = np.nan
        group[DURATION_RATIO_AVG] = value
        return group