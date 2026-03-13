import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "glue_scripts"))


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local")
        .appName("etl-tests")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
