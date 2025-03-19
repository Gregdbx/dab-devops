import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
from pyspark.testing.utils import assertDataFrameEqual
from bundle_demo.transformations.products import transform
from bundle_demo.commons.utils import get_spark

@pytest.fixture(scope="session")
def spark():
    """Fixture pour une session Spark unique pour tous les tests."""
    return get_spark()

@pytest.fixture
def mock_product_df(spark):
    """Fixture pour un DataFrame de test avec des produits."""
    schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True)
    ])
    
    data = [
        (1, "Laptop", "Electronics", 1200.0),
        (2, "Mouse", "Accessories", 25.5),
        (3, "Desk Chair", "Furniture", 150.0),
        (4, "Headphones", "Electronics", 85.99),
        (5, "Notebook", "Office", 5.99)
    ]
    
    return spark.createDataFrame(data, schema)

def test_transform_adds_expected_columns(spark, mock_product_df):
    """Test que la transformation ajoute les colonnes attendues."""
    transformed_df = transform(mock_product_df)
    expected_columns = {"product_id", "name", "category", "price", "price_with_tax", "price_category", "is_electronic"}
    assert set(transformed_df.columns) == expected_columns

def test_transform_calculates_tax_correctly(spark, mock_product_df):
    """Test que les transformations sont correctes."""
    transformed_df = transform(mock_product_df)
    expected_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("price_with_tax", DoubleType(), True),
        StructField("price_category", StringType(), False),
        StructField("is_electronic", BooleanType(), True)
    ])
    expected_data = [
        (1, "Laptop", "Electronics", 1200.0, 1440.00, "Premium", True),
        (2, "Mouse", "Accessories", 25.5, 30.60, "Budget", True),
        (3, "Desk Chair", "Furniture", 150.0, 180.00, "Standard", False),
        (4, "Headphones", "Electronics", 85.99, 103.19, "Standard", True),
        (5, "Notebook", "Office", 5.99, 7.19, "Budget", False)
    ]
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    assertDataFrameEqual(transformed_df, expected_df)

def test_transform_handles_missing_columns(spark):
    """Test que la transformation lève une erreur si des colonnes sont manquantes."""
    incomplete_df = spark.createDataFrame([(1, "Product1")], ["id", "name"])
    with pytest.raises(ValueError, match="Colonnes manquantes"):
        transform(incomplete_df)

