import pytest
from pyspark.sql import Row

from validator import validate_required_columns, drop_critical_nulls


def test_validate_columns_passes(spark):
    df = spark.createDataFrame([Row(order_id="1", customer_id="c1")])
    validate_required_columns(df, ["order_id", "customer_id"], "orders")


def test_validate_columns_raises_on_missing(spark):
    df = spark.createDataFrame([Row(order_id="1")])
    with pytest.raises(ValueError, match="Missing required columns"):
        validate_required_columns(df, ["order_id", "customer_id"], "orders")


def test_drop_critical_nulls_removes_rows(spark):
    df = spark.createDataFrame(
        [
            Row(order_id="1", price=10.0),
            Row(order_id=None, price=20.0),
        ]
    )
    result = drop_critical_nulls(df, ["order_id"], "order_items")
    assert result.count() == 1


def test_drop_critical_nulls_empty_cols_returns_unchanged(spark):
    df = spark.createDataFrame([Row(a=1, b=2)])
    result = drop_critical_nulls(df, [], "test")
    assert result.count() == 1
