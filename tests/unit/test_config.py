from config import CRITICAL_NULL_CHECKS, DATASET_FILES, FINAL_COLUMNS, REQUIRED_COLUMNS


def test_required_columns_structure():
    assert "customers" in REQUIRED_COLUMNS
    assert "orders" in REQUIRED_COLUMNS
    assert "order_items" in REQUIRED_COLUMNS
    assert "customer_id" in REQUIRED_COLUMNS["customers"]


def test_critical_null_checks():
    assert "orders" in CRITICAL_NULL_CHECKS
    assert "order_id" in CRITICAL_NULL_CHECKS["orders"]


def test_dataset_files():
    assert DATASET_FILES["customers"] == "olist_customers_dataset.csv"
    assert DATASET_FILES["orders"] == "olist_orders_dataset.csv"


def test_final_columns():
    assert "total_value" in FINAL_COLUMNS
    assert "order_id" in FINAL_COLUMNS
