from typing import Any, Dict, List

import pandas as pd

from .logger import CloudLogger


class DataValidation:
    """
    A utility class for validating data before further processing.

    Attributes:
        logger (CloudLogger): A logger instance for capturing log messages.
    """

    def __init__(self):
        """
        Initializes the DataValidation class.
        """
        self.logger = CloudLogger(__name__)

    def is_empty(self, data: List[Dict[str, Any]], table: str) -> bool:
        """
        Checks if the provided data is empty.

        Args:
            data (List[Dict[str, Any]]): A list of dictionaries representing the data.
            table (str): The name of the table being checked.

        Returns:
            bool: True if the data is empty, False otherwise.
        """
        if not data:
            self.logger.info(f"No data reveived for {table}")
            return True
        return False

    def validate_dataframe(
        self,
        data: List[Dict[str, Any]],
        required_columns: List[str],
        helper_log: str,
        type_checks: Dict[str, List[type]] = None,
    ) -> pd.DataFrame:
        """
        Validates the input data and converts it into a pandas DataFrame.

        Args:
            data (List[Dict[str, Any]]): A list of dictionaries representing the data.
            required_columns (List[str]): A list of required column names to validate.
            helper_log (str): A log string to identify the validation process.
            type_checks (Dict[str, List[type]], optional): A dictionary specifying
                column names as keys and a list of allowed data types as values.

        Returns:
            pd.DataFrame: A validated pandas DataFrame.

        Raises:
            ValueError: If required columns are missing from the data.
            TypeError: If any column has an incorrect data type.
        """
        if self.is_empty(data, helper_log):
            return pd.DataFrame()

        df = pd.DataFrame(data)

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        if type_checks:
            for col, allowed_types in type_checks.items():
                if col in df.columns:
                    if not any(isinstance(df[col].dtype, t) for t in allowed_types):
                        raise TypeError(
                            f"columns {col} has incorrect type: {df[col].dtype}"
                        )

        return df
