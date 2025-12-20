from python_files.utils3 import *
from pyspark.sql.functions import *
import pytest
from chispa.schema_comparer import assert_schema_equality


@pytest.fixture(scope = "session")
def sparkSession():
    return sparkSession.getOrCreate()


def test_add_loadts(spark):
    sample_df = spark.createDataFrame([(1,"charan") , (2, "pavan")], "id int, name string")
    correct_df = spark.createDataFrame([(1,"charan", current_timestamp()) , (2, "pavan", current_timestamp())], "id int, name string, load_ts timestamp")

    assert_schema_equality(add_loadts(sample_df), correct_df)


