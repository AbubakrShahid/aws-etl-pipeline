"""
Configuration constants for the Brazilian E-Commerce ETL pipeline.
Centralizes schema definitions, validation rules, and output structure.
"""

from typing import Dict, List

REQUIRED_COLUMNS: Dict[str, List[str]] = {
    "customers": [
        "customer_id",
        "customer_city",
        "customer_state",
    ],
    "orders": [
        "order_id",
        "customer_id",
        "order_status",
    ],
    "order_items": [
        "order_id",
        "price",
        "freight_value",
    ],
}

CRITICAL_NULL_CHECKS: Dict[str, List[str]] = {
    "orders": ["order_id", "customer_id"],
    "order_items": ["order_id", "price"],
}

VALID_ORDER_STATUSES: List[str] = ["delivered"]

FINAL_COLUMNS: List[str] = [
    "order_id",
    "customer_id",
    "customer_city",
    "customer_state",
    "order_status",
    "order_purchase_timestamp",
    "price",
    "freight_value",
    "total_value",
]

DATASET_FILES: Dict[str, str] = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
}

MIN_INPUT_ROWS: int = 1
