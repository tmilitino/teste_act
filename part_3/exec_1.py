"""Module for executing specific data processing tasks.

This module defines the `Exec1` class, which extends `ExecBase` to perform data transformations and partitioning by date.
"""
from pyspark.sql.functions import col, date_format
from exec_base import ExecBase
import logging


logger = logging.getLogger(__name__)

class Exec1(ExecBase):
    """A specific implementation of the `ExecBase` class for transforming and partitioning CSV data by date.

    Args:
        ExecBase: The base class providing session management and directory handling.
    """
    def execute(self):
        """Executes the pipeline to partitioning task.

        This method reads a CSV file, adds columns for year, month, and day extracted from the `create_date`,
        and saves the resulting DataFrame partitioned by year, month, and day to the output directory.
        """
        logger.info(f"PARTE 3 {self.__class__.__name__}")
        spark_session = self.get_session()

        df_exec_1 = spark_session.read.option('header', True)\
            .csv(f"{self.inputs_dir}/owners.csv")

        df_exec_1 = df_exec_1.withColumn('year', date_format(col("create_date"), "yyyy"))
        df_exec_1 = df_exec_1.withColumn('month', date_format(col("create_date"), "MM"))
        df_exec_1 = df_exec_1.withColumn('day', date_format(col("create_date"), "dd"))


        df_exec_1.repartition(1)\
            .write\
            .format("csv")\
            .option("header",True)\
            .partitionBy("year", "month", "day",)\
            .mode('overwrite')\
            .save(f"{self.outputs_dir}/part_3/exec_1")
        logger.info("Finished")
