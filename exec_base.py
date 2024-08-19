"""Base class for executing data processing tasks with Spark.

This module defines the `ExecBase` class, which is an abstract base class for executing 
data processing tasks using Apache Spark. It includes methods for managing Spark sessions, 
retrieving sample data, and creating DataFrames.

Classes:
    ExecBase (ABC): An abstract base class for defining data processing tasks in Spark.

Methods:
    execute():
        An abstract method that should be implemented by subclasses to define the 
        data processing logic.

    get_session():
        Retrieves or creates a Spark session.

    get_data():
        Provides sample data and column names for testing purposes.

    get_data_frame():
        Creates a Spark DataFrame from the sample data provided by `get_data()`.
"""

from collections import namedtuple
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import logging


Base = namedtuple("Base", ["data", "columns"])
logger = logging.getLogger(__name__)

class ExecBase(ABC):
    """Abstract base class for executing data-related tasks and Spark operations.

    Attributes:
        inputs_dir (str): Directory for input data.
        outputs_dir (str): Directory for output data.
    """

    def __init__(self):
        self.inputs_dir = "./inputs_data"
        self.outputs_dir = "./outputs_data"

    @abstractmethod
    def execute(self):
        """Abstract method for executing a specific task.

        This method must be implemented by subclasses.
        """
        ...

    def get_session(self) -> SparkSession:
        """Creates or retrieves an active SparkSession.

        Returns:
            SparkSession: The active SparkSession.
        """
        spark_session = SparkSession.builder.getOrCreate()
        return spark_session


    def get_data(self) -> Base:
        """Retrieves data and column names for creating a DataFrame.

        Returns:
            Base: A namedtuple containing the data and column names.
        """       
        data = [
            ("Alice", 34, "Data Scientist"),
            ("Bob", 45, "Data Engineer"),
            ("Cathy", 29, "Data Analyst"),
            ("David", 35, "Data Scientist")
        ]
        columns = ["Name", "Age", "Occupation"]

        return Base(data=data, columns=columns)

    def get_data_frame(self)-> DataFrame:
        """Creates a DataFrame from the provided data and columns.

        Returns:
            DataFrame: The created DataFrame.
        """
        spark_session = self.get_session()
        base_data = self.get_data()

        df_base = spark_session.createDataFrame(base_data.data, base_data.columns)

        return df_base
