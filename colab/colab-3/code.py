# CSE547 - Colab 3
# K-Means & PCA
# Adapted From Stanford's CS246

# Setup
# Let's setup Spark on your Colab environment. Run the cell below!
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging

logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', 
                    level=logging.INFO, 
                    handlers=[
                            logging.FileHandler("./log/intro.log", "w+"),
                            logging.StreamHandler()
                    ]
)

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
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

# Data Preprocessing
# In this Colab, we will load a famous machine learning dataset, the Breast Cancer Wisconsin dataset, using the scikit-learn datasets loader.
from sklearn.datasets import load_breast_cancer
breast_cancer = load_breast_cancer()

# For convenience, given that the dataset is small, we first construct a Pandas dataframe, tune the schema, and then convert it into a Spark dataframe.
pd_df = pd.DataFrame(breast_cancer.data, columns=breast_cancer.feature_names)
df = spark.createDataFrame(pd_df)

def set_df_columns_nullable(spark, df, column_list, nullable=False):
    for struct_field in df.schema:
        if struct_field.name in column_list:
            struct_field.nullable = nullable
    df_mod = spark.createDataFrame(df.rdd, df.schema)
    return df_mod

df = set_df_columns_nullable(spark, df, df.columns)
df = df.withColumn('features', array(df.columns))
vectors = df.rdd.map(lambda row: Vectors.dense(row.features))
d = df._jdf.schema().treeString()
logging.info("Step 1: df.printSchema: {}".format(d))
# You can also show the schema by d.printSchema() function but the output is not a string.

# With the next cell, we build the two datastructures that we will be using throughout this Colab:
# 1. features, a dataframe of Dense vectors, containing all the original features in the dataset;
# 2. labels, a series of binary labels indicating if the corresponding set of features belongs to a subject with breast cancer, or not.
from pyspark.ml.linalg import Vectors
features = spark.createDataFrame(vectors.map(Row), ["features"])
labels = pd.Series(breast_cancer.target)

# Your task!
# If you run successfully the Setup and Data Preprocessing stages, you are now ready to cluster the data with the K-means algorithm included in MLlib (Spark's Machine Learning library). Set the k parameter to 2, use a seed of 1, fit the model, and the compute the Silhouette score (i.e., a measure of quality of the obtained clustering).
# IMPORTANT: use the MLlib implementation of the Silhouette score (via ClusteringEvaluator).
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
# YOUR CODE HERE

# Take the predictions produced by K-means, and compare them with the labels variable (i.e., the ground truth from our dataset).
# Compute how many data points in the dataset have been clustered correctly (i.e., positive cases in one cluster, negative cases in the other).
# HINT: you can use np.count_nonzero(series_a == series_b) to quickly compute the element-wise comparison of two series.
# IMPORTANT: K-means is a clustering algorithm, so it will not output a label for each data point, but just a cluster identifier! As such, label 0 does not necessarily match the cluster identifier 0.
# YOUR CODE HERE

# Now perform dimensionality reduction on the features using the PCA statistical procedure, available as well in MLlib.
# Set the k parameter to 2, effectively reducing the dataset size of a 15X factor.
from pyspark.ml.feature import PCA
# YOUR CODE HERE

# Now run K-means with the same parameters as above, but on the pcaFeatures produced by the PCA reduction you just executed. (Again with a seed of 1)
# Compute the Silhouette score, as well as the number of data points that have been clustered correctly.
# YOUR CODE HERE

# Once you obtained the desired results, head over to Gradescope and submit your solution for this Colab!