"""Module for performing data operations with PySpark.

This module defines the `Exec1` class, which extends `ExecBase` to handle data reading and writing tasks.
"""
from exec_base import ExecBase
import logging


logger = logging.getLogger(__name__)

class Exec1(ExecBase):
    """A specific implementation of the `ExecBase` class for handling data read and write operations.

    Args:
        ExecBase: The base class providing session management and directory handling.
    """
    def execute(self):
        """Reads a CSV file and writes it in Parquet format.

        This method reads a CSV file from the input directory, then writes it to the output directory in Parquet format,
        repartitioning the DataFrame to a single partition before saving.
        """
        logger.info(f"PARTE 4 {self.__class__.__name__}")
        spark_session = self.get_session()

        df_exec_1 = spark_session.read.option('header', True)\
            .csv(f"{self.inputs_dir}/owners.csv")
        
        df_exec_1.repartition(1)\
            .write\
            .format("parquet")\
            .option("header",True)\
            .mode('overwrite')\
            .save(f"{self.outputs_dir}/part_4/exec_1")
        logger.info("Finished")
