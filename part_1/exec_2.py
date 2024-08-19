"""Module for executing specific data processing tasks.

This module defines the `Exec2` class, which extends `ExecBase` and performs data processing tasks.
"""
from pyspark.sql.functions import avg
from exec_base import ExecBase
import logging


logger = logging.getLogger(__name__)

class Exec2(ExecBase):
    """A specific implementation of the `ExecBase` class for processing data.

    Args:
        ExecBase (ABC): The abstract base class for execution tasks.
    """
    def execute(self):
        """Executes the pipeline to processing task.

        This method retrieves a DataFrame, calculates the average age for each occupation,
        and saves the result as a CSV file.
        """
        logger.info(f"PARTE 1 {self.__class__.__name__}")

        df_exec_2 = self.get_data_frame()

        df_exec_2_select = df_exec_2.groupBy("Occupation").agg(
            avg("Age").alias("Average Age Occupation"))

        df_exec_2_select.repartition(1)\
            .write\
            .format("csv")\
            .option("header", True)\
            .mode('overwrite')\
            .save(f"{self.outputs_dir}/part_1/exec_2")
        logger.info("Finished")
