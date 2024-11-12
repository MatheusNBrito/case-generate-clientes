from pyspark.sql import SparkSession

class SparkSessionWrapper:
    def __init__(self, app_name="GenerateClientes", master="local[*]"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY") \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
            .config("spark.hadoop.io.nativeio", "false") \
            .master(master) \
            .getOrCreate()

    def stop(self):
        self.spark.stop()
