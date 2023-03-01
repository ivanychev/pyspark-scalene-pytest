from pyspark.sql import SparkSession

from lib import duplicate


def test_duplicate():
    spark = (SparkSession.builder.master('local[1]')
             .appName('pytest')
             .enableHiveSupport()
             .getOrCreate())

    df = spark.createDataFrame([
        {"count": 1},
        {"count": 2},
    ])

    assert duplicate(df).count() == 4
