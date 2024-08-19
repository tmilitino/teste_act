"""Module for filtering and writing data with PySpark.

This module defines the `Exec2` class, which extends `ExecBase` to filter and write data using a SparkSession.
"""
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from exec_base import ExecBase

class Exec2(ExecBase):
    """Filters data and writes it to HDFS in Parquet format.

    Args:
        ExecBase: The base class providing session management and directory handling.
    """

    def execute(self):
        """Filters and writes the data.

        This method reads a Parquet file from HDFS, filters rows where the value in `column_name` is greater than 100,
        and writes the resulting DataFrame back to HDFS in Parquet format.
        """
        spark_session = self.get_session()

        input_path = "hdfs:///user/hadoop/inputs_data/"
        output_path = "hdfs:///user/hadoop/outputs_data/"

        df = spark_session.read.parquet(input_path)

        df_filtered = df.filter(col("column_name") > 100)

        df_filtered\
            .write\
            .format("parquet")\
            .option("header",True)\
            .mode('overwrite')\
            .save(output_path)
