from pyspark.dbutils import DBUtils
import os
import json
from pyspark.sql import SparkSession, DataFrame

def get_spark() -> SparkSession:
  """
  Get the current Spark session.
  """
  try:
    from databricks.connect import DatabricksSession
    spark = (DatabricksSession.builder.getOrCreate())
    return spark
  except ImportError:
    return SparkSession.builder.getOrCreate()


def get_user_name():
    """
    Get the username from the current Spark session.
    """
    current_user = get_spark().sql("SELECT current_user() as user").collect()[0]["user"]
    username = current_user.split('@')[0].lower().replace(".","_")
    return username


def get_workflow_name(dbutils):
    """
    Get the workflow name from the Databricks context.
    """
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    context_json = json.loads(context.toJson())
    tags = context_json.get("tags", {})
    workflow_name = tags.get("jobName", "")
    return workflow_name


def get_catalog():
    """
    Get the catalog name based on the current user and workflow name.
    """
    username = get_user_name()
    try:
      dbutils = DBUtils(get_spark())
      workflow_name = get_workflow_name(dbutils)
      if username in workflow_name:
        catalog_name = f"sandbox_{username}"
        print("Use sandbox catalog :",catalog_name)
      else:
          catalog_name = dbutils.widgets.get("catalog_name")  # From DAB job
    except Exception as e:
        # Sandbox: Construct catalog from username
        catalog_name = f"sandbox_{username}"
    return catalog_name