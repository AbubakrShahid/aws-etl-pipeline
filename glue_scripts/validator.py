"""
Validation logic for the ETL pipeline: column presence, null handling, and schema checks.
"""

import logging
from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)


def validate_required_columns(
    df: DataFrame,
    required_cols: List[str],
    df_name: str,
) -> None:
    """
    Verify all required columns exist in the DataFrame.
    Raises ValueError if any column is missing.
    """
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"[{df_name}] Missing required columns: {missing}")
    logger.info("[%s] Column validation passed", df_name)


def drop_critical_nulls(
    df: DataFrame,
    critical_cols: List[str],
    df_name: str,
) -> DataFrame:
    """
    Drop rows where critical columns are null. Logs a warning for each column with nulls.
    Returns the cleaned DataFrame.
    """
    for column in critical_cols:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            logger.warning(
                "[%s] Found %d null values in '%s' — dropping those rows",
                df_name,
                null_count,
                column,
            )
    if critical_cols:
        return df.dropna(subset=critical_cols)
    return df


def validate_all(
    dataframes: Dict[str, DataFrame],
    required_columns: Dict[str, List[str]],
    critical_null_checks: Dict[str, List[str]],
) -> Dict[str, DataFrame]:
    """
    Run all validation steps on the input DataFrames.
    Returns a dict of cleaned DataFrames with nulls dropped.
    """
    cleaned = {}
    for name, df in dataframes.items():
        validate_required_columns(df, required_columns[name], name)
        cleaned[name] = drop_critical_nulls(
            df, critical_null_checks.get(name, []), name
        )
    return cleaned


def validate_non_empty(df: DataFrame, df_name: str) -> None:
    """Raise ValueError if the DataFrame is empty."""
    count = df.count()
    if count == 0:
        raise ValueError(f"[{df_name}] Dataset is empty; cannot proceed")
