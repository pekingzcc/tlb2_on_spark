import unittest
from pyspark.sql import SparkSession
from tlb2_on_spark.read_file import read_moneta_file, agg_partition, process_by_tlb

class TestReadFile(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("tlb2_on_spark_test").getOrCreate()
        cls.path = "test_path"
        cls.ck = "test_ck"

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_read_moneta_file_valid_data(self):
        df = read_moneta_file(self.spark, self.path, self.ck)
        self.assertIsNotNone(df)
        self.assertIn("partition_id", df.columns)

    def test_read_moneta_file_invalid_ck(self):
        df = read_moneta_file(self.spark, self.path, "invalid_ck")
        self.assertEqual(df.count(), 0)

    def test_agg_partition_valid_data(self):
        df = read_moneta_file(self.spark, self.path, self.ck)
        df_agg = agg_partition(df)
        self.assertIsNotNone(df_agg)
        self.assertIn("buffer", df_agg.columns)

    def test_process_by_tlb_valid_data(self):
        df = read_moneta_file(self.spark, self.path, self.ck)
        df_agg = agg_partition(df)
        df_tlb = process_by_tlb(df_agg)
        self.assertIsNotNone(df_tlb)
        self.assertIn("eventName", df_tlb.columns)

if __name__ == "__main__":
    unittest.main()