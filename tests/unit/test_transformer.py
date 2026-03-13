from pyspark.sql import Row

from transformer import (
    add_total_value,
    apply_all,
    cast_numeric_columns,
    drop_invalid_numeric_rows,
    filter_delivered_orders,
    normalize_state,
)


def test_filter_delivered_orders(spark):
    df = spark.createDataFrame(
        [
            Row(order_status="delivered"),
            Row(order_status="cancelled"),
        ]
    )
    result = filter_delivered_orders(df)
    assert result.count() == 1


def test_add_total_value(spark):
    df = spark.createDataFrame([Row(price=10.0, freight_value=5.0)])
    result = add_total_value(df)
    assert result.collect()[0]["total_value"] == 15.0


def test_normalize_state(spark):
    df = spark.createDataFrame([Row(customer_state="sp")])
    result = normalize_state(df)
    assert result.collect()[0]["customer_state"] == "SP"


def test_cast_numeric_columns(spark):
    df = spark.createDataFrame([Row(price="10.5", freight_value="2.5")])
    result = cast_numeric_columns(df)
    assert result.schema["price"].dataType.typeName() == "double"


def test_drop_invalid_numeric_rows_removes_malformed(spark):
    df = spark.createDataFrame(
        [
            Row(order_id="1", price=10.0, freight_value=2.0),
            Row(order_id="2", price=None, freight_value=1.0),
        ]
    )
    result = drop_invalid_numeric_rows(df, ["price", "freight_value"], "order_items")
    assert result.count() == 1


def test_filter_delivered_returns_empty_df(spark):
    df = spark.createDataFrame(
        [
            Row(order_status="cancelled"),
            Row(order_status="shipped"),
        ]
    )
    result = filter_delivered_orders(df)
    assert result.count() == 0


def test_cast_numeric_handles_null_input(spark):
    """On Spark 3.3 (AWS Glue), cast() returns null for malformed input.
    This test verifies nulls are preserved when casting (works across Spark versions).
    """
    df = spark.createDataFrame(
        [
            Row(price="10.5", freight_value="2.0"),
            Row(price=None, freight_value="1.0"),
        ]
    )
    result = cast_numeric_columns(df)
    rows = result.collect()
    assert rows[0]["price"] == 10.5 and rows[0]["freight_value"] == 2.0
    assert rows[1]["price"] is None and rows[1]["freight_value"] == 1.0


def test_apply_all_returns_empty_when_no_delivered_orders(spark):
    orders = spark.createDataFrame(
        [("o1", "c1", "cancelled", "2020-01-01 00:00:00")],
        ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
    )
    order_items = spark.createDataFrame(
        [("o1", 1, 10.0, 2.0)],
        ["order_id", "order_item_id", "price", "freight_value"],
    )
    customers = spark.createDataFrame(
        [("c1", "city", "sp")],
        ["customer_id", "customer_city", "customer_state"],
    )
    result = apply_all(customers, orders, order_items)
    assert result.count() == 0
