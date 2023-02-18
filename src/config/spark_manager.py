from pyspark.sql import SparkSession

spark_session = (SparkSession.builder.master('local[*]').appName('complaint')
    .config("spark.executor.instances", "1") 
    .config("spark.executor.memory", "6g") 
    .config("spark.driver.memory", "6g") 
    .config("spark.executor.memoryOverhead", "8g")
    .getOrCreate())
