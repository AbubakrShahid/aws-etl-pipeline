"""
Transformation logic for the ETL pipeline: deduplication, filtering, type casting, and joins.
Handles schema drift and malformed data defensively.
"""

import logging
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    lit,
    round as spark_round,
    to_timestamp,
    upper,
)

logger = logging.getLogger(__name__)


def remove_duplicates(df: DataFrame, subset: List[str], name: str) -> DataFrame:
    """
    Remove duplicate rows based on the given columns.
    Logs the number of duplicates removed.
    """
    before = df.count()
    df = df.dropDuplicates(subset)
    after = df.count()
    if before != after:
        logger.info("[%s] Removed %d duplicate rows", name, before - after)
    return df


def filter_delivered_orders(df: DataFrame) -> DataFrame:
    """Filter to only include orders with status 'delivered'."""
    df = df.filter(col("order_status") == "delivered")
    logger.info("Orders after delivered filter: %d", df.count())
    return df


def cast_numeric_columns(df: DataFrame) -> DataFrame:
    """
    Cast price and freight_value to double. Invalid values become null.
    Handles schema drift (e.g. 'N/A' strings) by producing nulls for malformed data.
    """
    return df.withColumn("price", col("price").cast("double")).withColumn(
        "freight_value", col("freight_value").cast("double")
    )


def drop_invalid_numeric_rows(
    df: DataFrame, columns: List[str], df_name: str
) -> DataFrame:
    """
    Drop rows where numeric columns became null after cast (malformed input).
    """
    for col_name in columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            logger.warning(
                "[%s] Dropping %d rows with invalid/malformed values in '%s'",
                df_name,
                null_count,
                col_name,
            )
    return df.dropna(subset=columns)


def add_total_value(df: DataFrame) -> DataFrame:
    """Add total_value as price + freight_value, rounded to 2 decimals."""
    return df.withColumn(
        "total_value",
        spark_round(
            coalesce(col("price"), lit(0.0)) + coalesce(col("freight_value"), lit(0.0)),
            2,
        ),
    )


def normalize_state(df: DataFrame) -> DataFrame:
    """Normalize customer_state to uppercase."""
    return df.withColumn(
        "customer_state",
        upper(coalesce(col("customer_state"), lit(""))),
    )


def cast_timestamps(df: DataFrame) -> DataFrame:
    """
    Cast order_purchase_timestamp to timestamp type.
    Invalid values become null; nulls are preserved.
    """
    return df.withColumn(
        "order_purchase_timestamp",
        to_timestamp(col("order_purchase_timestamp")),
    )


def apply_all(
    customers_df: DataFrame,
    orders_df: DataFrame,
    order_items_df: DataFrame,
) -> DataFrame:
    """
    Apply all transformations and return the final joined DataFrame.
    Handles empty inputs and malformed data defensively.
    """
    orders_df = remove_duplicates(orders_df, ["order_id"], "orders")
    order_items_df = remove_duplicates(
        order_items_df, ["order_id", "order_item_id"], "order_items"
    )
    customers_df = remove_duplicates(customers_df, ["customer_id"], "customers")

    orders_df = filter_delivered_orders(orders_df)

    order_items_df = cast_numeric_columns(order_items_df)
    order_items_df = drop_invalid_numeric_rows(
        order_items_df, ["price", "freight_value"], "order_items"
    )

    orders_df = cast_timestamps(orders_df)

    order_items_df = add_total_value(order_items_df)

    customers_df = normalize_state(customers_df)

    joined_df = order_items_df.join(orders_df, "order_id", "inner").join(
        customers_df, "customer_id", "inner"
    )

    logger.info("Joined row count: %d", joined_df.count())
    return joined_df
