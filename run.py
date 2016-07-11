# -*- coding: utf-8 -*-
import os
import inspect
import findspark
import time

findspark.init()

import pytest

from pyspark import SparkContext, HiveContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# pylint:disable=redefined-outer-name
# This cannot be avoided by the way pytest fixtures work

@pytest.fixture(scope='session')
def spark_context(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object
    Returns:
        SparkContext for tests
    """
    sc = SparkContext(appName='pytest-pyspark-local-testing')

    def shutdown():
        sc.stop()

    request.addfinalizer(shutdown)

    return sc


@pytest.fixture(scope='session')
def sql_context(spark_context):
    """  fixture for creating a Hive Context. Creating a fixture enables it to be reused across all
        tests in a session
    Args:
        spark_context: spark_context fixture
    Returns:
        HiveContext for tests
    """
    return HiveContext(spark_context)

TABLE_NAME = 'people'


def num_rows(sql_context):
    return int(sql_context.sql('SELECT COUNT(*) ct FROM {0}'.format(TABLE_NAME)).first().ct)

def test_sql(sql_context):
    SCHEMA = StructType([
        StructField('name', StringType(), False),
        StructField('age', IntegerType(), False),
        StructField('country', StringType(), False)
    ])

    sql_context.sql('DROP TABLE IF EXISTS {0}'.format(TABLE_NAME));
    sql_create = """
    CREATE TABLE {0} (
      name    STRING,
      age     INT,
      country STRING
    ) STORED AS PARQUET
    """.format(TABLE_NAME)
    sql_context.sql(sql_create)

    (sql_context
        .createDataFrame([('Piter', 19, 'FRL')], SCHEMA)
        .write
        .saveAsTable(TABLE_NAME, format='parquet', mode='append'))

    assert num_rows(sql_context) == 1 # Just Piter

    (sql_context
        .createDataFrame([('Gurbe', 25, 'FRL')], SCHEMA)
        .write
        .saveAsTable(TABLE_NAME, format='parquet', mode='append'))

    assert num_rows(sql_context) == 2 # Works fine

def test_sql_partition(sql_context):
    SCHEMA = StructType([
        StructField('name', StringType(), False),
        StructField('age', IntegerType(), False)
    ])

    sql_context.sql('DROP TABLE IF EXISTS {0}'.format(TABLE_NAME));
    sql_create = """
    CREATE TABLE {0} (
      name STRING,
      age  INT
    ) PARTITIONED BY (country STRING) STORED AS PARQUET
    """.format(TABLE_NAME)
    sql_context.sql(sql_create)

    (sql_context
        .createDataFrame([('Piter', 19)], SCHEMA)
        .write
        .saveAsTable('{0}_staging'.format(TABLE_NAME), format='parquet', mode='overwrite'))
    sql_context.sql('INSERT INTO {0} PARTITION(country=\'FRL\') SELECT * FROM {0}_staging'.format(TABLE_NAME))

    assert num_rows(sql_context) == 1 # Just Piter

    (sql_context
        .createDataFrame([('Gurbe', 25)], SCHEMA)
        .write
        .saveAsTable('{0}_staging'.format(TABLE_NAME), mode='overwrite'))
    sql_context.sql('INSERT INTO {0} PARTITION(country=\'FRL\') SELECT * FROM {0}_staging'.format(TABLE_NAME))

    assert num_rows(sql_context) == 1 # Should be Piter and Gurbe

    sql_context.refreshTable(TABLE_NAME)

    assert num_rows(sql_context) == 2 # Now it is


def test_api_partition(sql_context):
    SCHEMA = StructType([
        StructField('name', StringType(), False),
        StructField('age', IntegerType(), False),
        StructField('country', StringType(), False)
    ])

    sql_context.sql('DROP TABLE IF EXISTS {0}'.format(TABLE_NAME));
    sql_create = """
    CREATE TABLE {0} (
      name STRING,
      age  INT
    ) PARTITIONED BY (country STRING) STORED AS PARQUET
    """.format(TABLE_NAME)
    sql_context.sql(sql_create)

    sql_context.setConf('hive.exec.dynamic.partition.mode','nonstrict')

    (sql_context
        .createDataFrame([('Piter', 19, 'FRL')], SCHEMA)
        .write
        .mode('append')
        .partitionBy('country')
        .insertInto(TABLE_NAME))

    assert num_rows(sql_context) == 1 # Just Piter

    (sql_context
        .createDataFrame([('Gurbe', 25, 'FRL')], SCHEMA)
        .write
        .mode('append')
        .partitionBy('country')
        .insertInto(TABLE_NAME))

    assert num_rows(sql_context) == 1 # Should be Piter and Gurbe

    sql_context.refreshTable(TABLE_NAME)

    assert num_rows(sql_context) == 2 # Now it is
