import pytest
from unittest.mock import Mock, patch
import os
import json
from bundle_demo.commons.utils import get_user_name, get_catalog, get_workflow_name

# Mock get_spark fixture
@pytest.fixture
def mock_spark():
    spark = Mock()
    spark.sql.return_value.collect.return_value = [{"user": "gregoire.portier@databricks.com"}]
    return spark

# Mock dbutils dev, staging, prod
@pytest.fixture
def mock_dbutils_deployed():
    dbutils = Mock()
    # Mock context for deployed mode
    context = Mock()
    context.toJson.return_value = json.dumps({
        "tags": {"jobName": "prod_bundle_demo_job"}  # No username in workflow_name
    })
    dbutils.notebook.entry_point.getDbutils.return_value.notebook.return_value.getContext.return_value = context
    dbutils.widgets.get.return_value = "prod"
    return dbutils


# Mock dbutils sandbox
@pytest.fixture
def mock_dbutils_deployed_sandbox():
    dbutils = Mock()
    context = Mock()
    context.toJson.return_value = json.dumps({
        "tags": {"jobName": "[dev gregoire_portier] sandbox_bundle_demo_job"}  #  username in workflow_name
    })
    dbutils.notebook.entry_point.getDbutils.return_value.notebook.return_value.getContext.return_value = context
    dbutils.widgets.get.return_value = None
    return dbutils

# Test pour récuperer le nom de l'utisateur
def test_get_user_name(mock_spark):
    with patch("bundle_demo.commons.utils.get_spark", return_value=mock_spark):
        assert get_user_name() == "gregoire_portier"

# Test pour récuperer le nom du workflow en production
def test_get_workflow_name(mock_dbutils_deployed):
    workflow_name = get_workflow_name(mock_dbutils_deployed)
    assert workflow_name == "prod_bundle_demo_job"


# Test pour récuperer le nom du workflow en production
def test_get_catalog_deployed_prod(mock_spark, mock_dbutils_deployed):
    with patch("bundle_demo.commons.utils.get_spark", return_value=mock_spark):
        with patch("bundle_demo.commons.utils.DBUtils", return_value=mock_dbutils_deployed):
            with patch("bundle_demo.commons.utils.get_user_name", return_value="gregoire_portier"):
                catalog = get_catalog()
                assert catalog == "prod"


# Test pour récuperer le nom du catalog en production
def test_get_catalog_deployed(mock_spark, mock_dbutils_deployed):
    with patch("bundle_demo.commons.utils.get_spark", return_value=mock_spark):
        with patch("bundle_demo.commons.utils.DBUtils", return_value=mock_dbutils_deployed):
            with patch("bundle_demo.commons.utils.get_user_name", return_value="gregoire_portier"):
                catalog = get_catalog()
                assert catalog == "prod"

# Test pour récuperer le nom du catalog en sandbox
def test_get_catalog_sandbox_workflow(mock_spark, mock_dbutils_deployed_sandbox):
    with patch("bundle_demo.commons.utils.get_spark", return_value=mock_spark):
        with patch("bundle_demo.commons.utils.DBUtils", return_value=mock_dbutils_deployed_sandbox):
            with patch("bundle_demo.commons.utils.get_user_name", return_value="gregoire_portier"):
                catalog = get_catalog()
                assert catalog == "sandbox_gregoire_portier"


# Test pour récuperer le nom du catalog sans contexte
def test_get_catalog_sandbox_no_context(mock_spark):
    with patch("bundle_demo.commons.utils.get_spark", return_value=mock_spark):
        with patch("bundle_demo.commons.utils.DBUtils", side_effect=ImportError("No Databricks")):
            with patch("bundle_demo.commons.utils.get_user_name", return_value="gregoire_portier"):
                catalog = get_catalog()
                assert catalog == "sandbox_gregoire_portier"

