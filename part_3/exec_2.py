"""Module for executing data join operations with PySpark.

This module defines the `Exec2` class, which extends `ExecBase` to perform join operations between large and smaller DataFrames.
"""
from pyspark.sql.functions import broadcast
from exec_base import ExecBase
import logging


logger = logging.getLogger(__name__)

class Exec2(ExecBase):
    """A specific implementation of the `ExecBase` class for joining large and small DataFrames.

    Args:
        ExecBase: The base class providing session management and directory handling.
    """
    def execute(self):
        """Executes the pipeline to use a broadcast join.

        This method reads two CSV files, performs a join operation between a large DataFrame and a smaller DataFrame using broadcast join,
        and saves the resulting DataFrame as a CSV file.
        """
        logger.info(f"PARTE 3 {self.__class__.__name__}")
        spark_session = self.get_session()

        df_large = spark_session.read.option('header', True)\
            .csv(f"{self.inputs_dir}/owners.csv")
        df_smaller = spark_session.read.option('header', True)\
            .csv(f"{self.inputs_dir}/car_makers.csv")


        result = df_large.join(broadcast(df_smaller), df_large["car_maker_id"] == df_smaller["id"])
        result = result.drop(df_smaller["id"])

        result.repartition(1)\
            .write\
            .format("csv")\
            .option("header",True)\
            .mode('overwrite')\
            .save(f"{self.outputs_dir}/part_3/exec_2")
        logger.info("Finished")
