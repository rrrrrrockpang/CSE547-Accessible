# CSE 547- Colab 7
# Decision Trees on Spark
# Adapted From Stanford's CS246
# Setup
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', 
                    level=logging.INFO, 
                    handlers=[
                            logging.FileHandler("./log/intro.log", "w+"),
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
# In this Colab, we will be using the famous [MNIST database](https://en.wikipedia.org/wiki/MNIST_database), a large collection of handwritten digits that is widely used for training and testing in the field of machine learning.
# For your convenience, the dataset has already been converted to the popular LibSVM format, where each digit is represented as a sparse vector of grayscale pixel values.
training = spark.read.format("libsvm").option("numFeatures","784").load("mnist-digits-train.txt")
test = spark.read.format("libsvm").option("numFeatures","784").load("mnist-digits-test.txt")

# Cache data for multiple uses
training.cache()
test.cache()

training.show(truncate=False)
logging.info("Step 1: training.show(truncate=False): {}".format(training.show(truncate=False)))
logging.info("Step 1: training.printSchema(): {}".format(training._jdf.schema().treeString()))
logging.info("Step 1: test.printSchema(): {}".format(test._jdf.schema().treeString()))
# You can also show the schema by printSchema() function but the output is not a string.

# Your Task
# First of all, find out how many instances we have in our training / test split.
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer
# YOUR CODE HERE


# Now train a Decision Tree on the training dataset using Spark MLlib. Use the default parameters for your classifier (You can use a different labelCol name)
# You can refer to the Python example on this documentation page: https://spark.apache.org/docs/latest/ml-classification-regression.html#decision-tree-classifier
# Note: the `featureIndexer`` is unnecessary in the pipeline because the data is already in a format that we can work with and the `featuresCol`` for the DecisionTreeClassifier can be kept as the default value.
# YOUR CODE HERE


# With the Decision Tree you just induced on the training data, predict the labels of the test set. Print the predictions for the first 10 digits, and compare them with the labels.
# YOUR CODE HERE


# The small sample above looks good, but not great!
# Let's dig deeper. Compute the accuracy of our model, using the MulticlassClassificationEvaluator from MLlib.
# YOUR CODE HERE


# Find out the max depth of the trained Decision Tree, and its total number of nodes.
# YOUR CODE HERE


# It appears that the default settings of the Decision Tree implemented in MLlib did not allow us to train a very powerful model!
# Before starting to train a Decision Tree, you can tune the max depth it can reach using the [setMaxDepth()](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.classification.DecisionTreeClassifier.html#pyspark.ml.classification.DecisionTreeClassifier.setMaxDepth) method. Train 21 different DTs, varying the max depth from 0 to 20, endpoints included (i.e., [0, 20]). For each value of the parameter, print the accuracy achieved on the test set, and the number of nodes contained in the given DT.
# IMPORTANT: this parameter sweep can take 30 minutes or more, depending on how busy is your Colab instance. Notice how the induction time grows super-linearly!
# YOUR CODE HERE


# Once you have working code for each cell above, head over to Gradescope, read carefully the questions, and submit your solution for this Colab!