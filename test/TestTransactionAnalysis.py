import unittest
from pyspark.sql import SparkSession, Row
from pyspark.testing.utils import assertDataFrameEqual
from src.ExecuteOperations import transform_data


# Define unit test base class
class PySparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestTransactionAnalysis").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


# Define unit test
class TestTransformation(PySparkTestCase):
    def test_transformation(self):
        # Sample data
        sample_data = [
            {"user_id": "u1", "account_id": "u1a1", "counterparty_id": "u3", "transaction_type": "outgoing",
             "date": "2023-01-01", "amount": 100},
            {"user_id": "u1", "account_id": "u1a2", "counterparty_id": "u3", "transaction_type": "incoming",
             "date": "2023-01-02", "amount": 200},
            {"user_id": "u1", "account_id": "u1a2", "counterparty_id": "u4", "transaction_type": "outgoing",
             "date": "2023-01-03", "amount": 150},
            {"user_id": "u2", "account_id": "u2a1", "counterparty_id": "u5", "transaction_type": "outgoing",
             "date": "2023-01-02", "amount": 100},
            {"user_id": "u2", "account_id": "u2a1", "counterparty_id": "u6", "transaction_type": "outgoing",
             "date": "2023-01-04", "amount": 120},
            {"user_id": "u1", "account_id": "u1a1", "counterparty_id": "u4", "transaction_type": "incoming",
             "date": "2023-01-05", "amount": 200}
        ]

        # Create a Spark DataFrame from the sample data
        input_df = self.spark.createDataFrame(sample_data)

        # Apply the transformation function
        transformed_df = transform_data(input_df)

        # Expected data
        expected_data = [
            ("u1", "u4", 350),
            ("u2", "u6", 120)
        ]
        columns = ["user_id", "counterparty_id", "amount"]

        # Create a Spark DataFrame from the expected data
        expected_df = self.spark.createDataFrame(data=expected_data, schema=columns)

        # Assert that the DataFrames are equal
        assertDataFrameEqual(transformed_df, expected_df)
