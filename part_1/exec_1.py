"""Module for executing specific data processing tasks.

This module defines the `Exec1` class, which extends `ExecBase` to perform specific data transformations.
"""
from pyspark.sql.functions import col
from exec_base import ExecBase

import logging
logger = logging.getLogger(__name__)

class Exec1(ExecBase):
    """A specific implementation of the `ExecBase` class for processing data.

    Args:
        ExecBase (ABC): The abstract base class for execution tasks.
    """

    def execute(self):
        """Executes the data processing task.

        This method retrieves a DataFrame, selects the "Name" and "Age" columns,
        filters rows where the age is greater than 30, and saves the result as a CSV file.
        """
        logger.info(f"PARTE 1 {self.__class__.__name__}")
        df_exec_1 = self.get_data_frame()

        df_exec_1_select = df_exec_1.select("Name", "Age").filter(col("Age")>30)

        df_exec_1_select.repartition(1)\
            .write\
            .format("csv")\
            .option("header",True)\
            .mode('overwrite')\
            .save(f"{self.outputs_dir}/part_1/exec_1")
        logger.info("Finished")