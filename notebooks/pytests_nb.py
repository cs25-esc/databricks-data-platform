# Databricks notebook source

%run /Workspace/Users/charansairangasthalam1985@gmail.com/.bundle/databricks-data-platform/default/files/notebooks/lb_utilities


import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-spark")
        .getOrCreate()
    )


def test_add_loadts_schema(spark):
    sample_df = spark.createDataFrame(
        [(1, "charan"), (2, "pavan")],
        "id int, name string"
    )

    result_df = add_loadts(sample_df)

    expected_df = spark.createDataFrame(
        [(1, "charan", None), (2, "pavan", None)],
        "id int, name string, load_ts timestamp"
    )
    

    for i in result_df.columns():
        for j in expected_df.columns():
            if i != j:
                assert False

    assert True
