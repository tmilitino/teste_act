"""Module for processing action logs with PySpark.

This module defines the `ActionLog` class, which extends `ExecBase` to count actions per user and identify the top 10 users with the most actions.
"""
from pyspark.sql.functions import col, count
from exec_base import ExecBase
import logging


logger = logging.getLogger(__name__)

class ActionLog(ExecBase):
    """Counts user actions and identifies the top 10 users.

    Args:
        ExecBase: The base class providing session management and directory handling.
    """

    def execute(self):
        """Counts actions per user and saves the top 10 users.

        This method reads a CSV file containing action logs, counts the number of actions per user, identifies
        the top 10 users with the most actions, and writes the results to a CSV file.
        """
        logger.info(f"PARTE 5 {self.__class__.__name__}")
        spark_session = self.get_session()

        df_action_log = spark_session.read.option('header', True)\
            .csv(f"{self.inputs_dir}/action_logs.csv")

        df_action_log_counted = df_action_log.groupBy("user_id").agg(
            count(col("user_id")).alias("action_counter"))

        df_action_log_10_top = df_action_log_counted.orderBy(col("action_counter").desc()).limit(10)

        df_action_log_10_top.repartition(1)\
            .write\
            .format("csv")\
            .mode('overwrite')\
            .option("header",True)\
            .save(f"{self.outputs_dir}/part_5/action_counter")
        logger.info("Finished")
