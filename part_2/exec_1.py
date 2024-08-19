"""Module for age categorization in Spark.

Defines the `Exec1` class to categorize ages and save the results to CSV.
"""
from pyspark.sql.functions import udf
from exec_base import ExecBase
import logging


logger = logging.getLogger(__name__)

class Exec1(ExecBase):
    """Class for processing age data and saving results.

    Inherits from `ExecBase`. Categorizes ages and saves results to CSV.
    """

    @staticmethod
    def set_age_category(age:int):
        """Categorizes age into "Jovem", "Adulto", or "Senior".
        
        The age is categorized into:
            - "Jovem" if the age is less than 30
            - "Adulto" if the age is between 30 and 40 (inclusive)
            - "Senior" if the age is greater than 4

        Args:
            age (int): The age to categorize.

        Returns:
            str: The age category.
        """
        age_min_limit = 30
        age_max_limit = 40

        if age < age_min_limit:
            return "Jovem"

        elif age >= age_min_limit and age <= age_max_limit:
            return "Adulto"

        elif age > age_max_limit:
            return "Senior"

    def execute(self):
        """Executes the pipeline to categorize ages and save to CSV.

        Reads a DataFrame, applies age categorization, and writes the output to CSV.
        """
        
        logger.info(f"PARTE 2 {self.__class__.__name__}")
        df_exec_1 = self.get_data_frame()

        udf_set_age_category = udf(lambda x: Exec1.set_age_category(x))

        df_exec_1_select = df_exec_1.withColumn("Age Category", udf_set_age_category("Age"))

        df_exec_1_select.repartition(1)\
            .write\
            .format("csv")\
            .mode('overwrite')\
            .option("header",True)\
            .save(f"{self.outputs_dir}/part_2/exec_1")
        logger.info("Finished")
