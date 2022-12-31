# CSE547 - Colab 4
# Collaborative Filtering
# Adapted from Stanford's CS246

# Setup
# Let's setup Spark on your Colab environment. Run the cell below!
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging

logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', 
                    level=logging.INFO, 
                    handlers=[
                            logging.FileHandler("./log/code.log", "w+"),
                            logging.StreamHandler()
                    ]
)

import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf

# Let's initialize the Spark context.
# create the session
conf = SparkConf().set("spark.ui.port", "4050")

# create the context
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

# Data Loading
# In this Colab, we will be using the MovieLens dataset, specifically the 100K dataset (which contains in total 100,000 ratings from 1000 users on ~1700 movies).
# We load the ratings data in a 80%-20% training/test split, while the items dataframe contains the movie titles associated to the item identifiers.
schema_ratings = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("item_id", IntegerType(), False),
    StructField("rating", IntegerType(), False),
    StructField("timestamp", IntegerType(), False)])

schema_items = StructType([
    StructField("item_id", IntegerType(), False),
    StructField("movie", StringType(), False)])

training = spark.read.option("sep", "\t").csv("MovieLens.training", header=False, schema=schema_ratings)
test = spark.read.option("sep", "\t").csv("MovieLens.test", header=False, schema=schema_ratings)
items = spark.read.option("sep", "|").csv("MovieLens.item", header=False, schema=schema_items)
logging.info("Step 1: training schema: {}".format(training.printSchema()))
logging.info("Step 1: items schema: {}".format(items.printSchema()))

# Your Task
# Let's compute some stats! What is the number of ratings in the training and test dataset? How many movies are in our dataset?
# YOUR CODE HERE


# Using the training set, train models with the Alternating Least Squares method available in the Spark MLlib URL: https://spark.apache.org/docs/latest/ml-collaborative-filtering.html
# Model 1:
# * maximal iteration = 10
# * rank = 10
# * regularization = 0.1
# * Drop rows with a NaN value in rating
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
# YOUR CODE HERE


# Now compute the RMSE on the test dataset.
# YOUR CODE HERE

# To experiment with different values of rank, repeat the process of computing RMSE for the following models (Feel free to just edit the code you wrote above):
# Model 2:
# * Model 1 with rank = 5
# Model 3:
# * Model 1 with rank = 100
# YOUR CODE 


# Now experiment with different regularization values using Model 3 (rank = 100). Which regularization gives the lowest RMSE? (Feel free to just edit the code you wrote above)
# * 1
# * 0.3
# * 0.1
# * 0.03
# * 0.01
# YOUR CODE HERE


# At this point, you can use the trained model to produce the top-K recommendations for each user.
# Using model 3 (rank 100, regularization 0.1) find the most frequent #1 movie recommendation across all users. You may find it easier to convert the recommendation output to a Pandas Dataframe. Which movie is it?
# Hint: One approach is described on GradeScope.
# YOUR CODE HERE

# Once you have working code for each cell above, head over to Gradescope, read carefully the questions, and submit your solution for this Colab!