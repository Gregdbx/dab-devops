import pytest
from pyspark.testing.utils import assertDataFrameEqual
from bundle_demo.commons.utils import get_spark

@pytest.fixture(scope="session")
def spark():
    """Fixture pour une session Spark unique pour tous les tests."""
    return get_spark()

# Test table qui sont situées sur databricks
def test_query_table_on_databricks(spark):
    spark.table("users.gregoire_portier.table_for_test").count() == 5
