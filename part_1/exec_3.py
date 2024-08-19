"""Module for executing specific data processing tasks.

This module defines the `Exec3` class, which extends `ExecBase` to perform specific data transformations and aggregations.
"""
from pyspark.sql.functions import avg, col
from exec_base import ExecBase
import logging


logger = logging.getLogger(__name__)

class Exec3(ExecBase):
    """A specific implementation of the `ExecBase` class for processing and analyzing data.

    Args:
        ExecBase (ABC): The abstract base class for execution tasks.
    """
    def execute(self):
        """Executes the pipeline to processing task.

        This method retrieves a DataFrame, groups the data by "Occupation" to calculate the average age for each occupation,
        sorts the results by the average age in descending order, and saves the result as a CSV file.
        """

        logger.info(f"PARTE 1 {self.__class__.__name__}")

        df_exec_3 = self.get_data_frame()

        df_exec_3_select = df_exec_3.groupBy("Occupation").agg(avg("Age").alias("Average Age Occupation"))

        df_exec_3_select = df_exec_3_select.orderBy(col("Average Age Occupation").desc())

        df_exec_3_select.repartition(1)\
            .write\
            .format("csv")\
            .option("header", True)\
            .mode('overwrite')\
            .save(f"{self.outputs_dir}/part_1/exec_3")
        logger.info("Finished")