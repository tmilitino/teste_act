"""Module for executing specific data processing tasks.

This module defines the `Exec2` class, which extends `ExecBase` to perform data transformations and calculations using window functions.
"""
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window
from exec_base import ExecBase
import logging


logger = logging.getLogger(__name__)

class Exec2(ExecBase):
    """A specific implementation of the `ExecBase` class for processing and analyzing data with window functions.

    Args:
        ExecBase (ABC): The abstract base class for execution tasks.
    """

    def execute(self):
        """Executes the data processing task.

        This method retrieves a DataFrame, calculates the average age for each occupation using window functions,
        computes the difference between each individual's age and the average age for their occupation,
        and saves the result as a CSV file.
        """
        logger.info(f"PARTE 2 {self.__class__.__name__}")
        df_exec_2 = self.get_data_frame()

        window_spec_occupation = Window.partitionBy("Occupation")

        df_exec_2 = df_exec_2.withColumn("Average Age Occupation",
                                         avg("age").over(window_spec_occupation))
        df_exec_2 = df_exec_2.withColumn("Difference Age Average",
                                         col("Age") - col("Average Age Occupation"))

        df_exec_2.repartition(1)\
            .write\
            .format("csv")\
            .mode('overwrite')\
            .option("header", True)\
            .save(f"{self.outputs_dir}/part_2/exec_2")
        logger.info("Finished")
