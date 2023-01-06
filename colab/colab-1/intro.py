# Title: CSE547 - Colab 1
# Wordcount in Spark
# Adapted From Stanford CS246

# Step 1: Let's import the libraries we will need
import pyspark, logging
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf

# Housekeeping: import logging for intermediate results
def script_filter(record):
    if record.name != __name__:
        return False
    return True
    
handler = logging.StreamHandler()
handler.filters = [script_filter]
logger = logging.getLogger(__name__)
logger.addHandler(handler)
fh = logging.FileHandler('./log/section_1.log', "w+")
logger.setLevel(logging.INFO)
logger.addHandler(fh)
# End of Housekeeping

# Your Task!
# You can find pg100.txt in the data folder which contains a copy of the complete works of Shakespeare
# Write a Spark application which outputs the number of words that start with each letter. 
# This means that for every letter we want to count the total number of (non-unique) words that start with a specific letter. 
# In your implementation ignore the letter case, i.e., consider all words as lower case. Also, you can ignore all the words starting with a non-alphabetic character.
# For this task we ask you to the RDD MapReduce API from spark (map, reduceByKey, flatMap, etc.) instead of DataFrame API.
# You can find the RDD MapReduce API documentation here: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.html

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext
import pandas as pd

# create the Spark Session
spark = SparkSession.builder.config("spark.driver.memory", "4g").getOrCreate()

# create the Spark Context
sc = spark.sparkContext

# YOUR CODE HERE

# To view output of a spark dataframe, you can use the following code:
# You need to replace YOUR_SPARK_DATAFRAME
# pandas_df = YOUR_SPARK_DATAFRAME.toPandas()
# logger.info("logging output: ")
# logger.info('\t'+ pandas_df.head().to_string().replace('\n', '\n\t'))  # pandas_df is a Pandas dataframe
# Then you can go to the log folder and check the log file.
