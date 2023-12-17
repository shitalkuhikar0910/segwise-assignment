from pyspark.sql import SparkSession
from lib.ConfigReader import get_pyspark_config

def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .config(conf=get_pyspark_config(env)) \
            .master("local[2]") \
            .getOrCreate()
    else:
        print('two')
        return SparkSession.builder \
            .config(conf=get_pyspark_config(env)) \
            .enableHiveSupport() \
            .getOrCreate()
            
            
def create_hardcoded_spark_session():
    return SparkSession.builder \
        .appName("YourHardcodedAppName") \
        .config("spark.some.config.option", "some-value") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .getOrCreate()