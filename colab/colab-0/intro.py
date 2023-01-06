# Title: CSE547 - Colab 0
# Overview: Spark Tutorial
# In this tutorial you will learn how to use Apache Spark in local mode on a Colab enviroment.
# Credits to Tiziano Piccardi for his Spark Tutorial used in the Applied Data Analysis class at EPFL.
# Further adapted by CSE 547 from Stanford's CS246
# check ./log/intro.log for the output

# Step 1: Let's import the libraries we will need
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging

def script_filter(record):
    if record.name != __name__:
        return False
    return True
    
handler = logging.StreamHandler()
handler.filters = [script_filter]
logger = logging.getLogger(__name__)
logger.addHandler(handler)
fh = logging.FileHandler('./log/intro.log', "w+")
logger.setLevel(logging.INFO)
logger.addHandler(fh)

import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf

# Step 2: Let's initialize the Spark context.

## create the session
conf = SparkConf().set("spark.ui.port", "4050")

## create the context
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

# We are going to study spark under the context of the IMDB dataset
# The IMDB dataset describes TV, film, and other media titles listed on the IMDB site.
#
# This dataset consists of multiple tables, but we use only two for this lab. The below descriptions are copied from the IMDB dataset [interfaces page](https://www.imdb.com/interfaces/).
#
# Title Basic Information [IMDB link](https://datasets.imdbws.com/title.basics.tsv.gz)
# tconst (string) - alphanumeric unique identifier of the title
# titleType (string) – the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
# primaryTitle (string) – the more popular title / the title used by the filmmakers on promotional materials at the point of release
# originalTitle (string) - original title, in the original language
# isAdult (boolean) - 0: non-adult title; 1: adult title
# startYear (YYYY) – represents the release year of a title. In the case of TV Series, it is the series start year
# endYear (YYYY) – TV Series end year. ‘\N’ for all other title types
# runtimeMinutes – primary runtime of the title, in minutes
# genres (string array) – includes up to three genres associated with the title
#
# Principal Cast/Crew [IMDB link](https://datasets.imdbws.com/title.principals.tsv.gz)
# tconst (string) - alphanumeric unique identifier of the title
# ordering (integer) – a number to uniquely identify rows for a given titleId
# nconst (string) - alphanumeric unique identifier of the name/person
# category (string) - the category of job that person was in
# job (string) - the specific job title if applicable, else '\N'
# characters (string) - the name of the character played if applicable, else '\N'


## Step 3: Load the datasets
Titles = spark.read.csv("data/title.basics.tsv", sep='\t', header=True)
Principals = spark.read.csv("data/title.principals.tsv", sep='\t', header=True)

## Step 4: check the schema. (check logging)
logger.info("Step 4: {}".format(Titles._jdf.schema().treeString()))
logger.info("Step 4: {}".format(Principals._jdf.schema().treeString()))
# You can also show the schema by Titles.printSchema() function but the output is not a string.

# Step 5: take() returns a sample of rows (check logging)
logger.info("Step 5: {}".format(pd.DataFrame(Titles.take(3)).to_string().replace('\n', '\n\t')))

# Step 6: show() returns a formatted sample of rows. (check logging)
logger.info("Step 6: ")
logger.info(f"In total there are {Titles.count():,d} IMDB titles.")

# Step 7: We'll do a little bit of preprocessing here to remove any titles with a null title type or start year field.
Titles = Titles.replace({'\\N': None}).dropna(subset=['titleType', 'startYear'])
logger.info(f"Step 7: After preprocessing, there are {Titles.count():,d} IMDB titles.")
