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
fh = logging.FileHandler('./log/intro.log')
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

# We are going to study spark under the context of a dataset about the Vietname War
# President Johnson: What do you think about this Vietnam thing? I’d like to hear you talk a little bit.
# Senator Russell: Well, frankly, Mr. President, it’s the damn worse mess that I ever saw, and I don’t like to brag and I never have been right many times in my life, but I knew that we were going to get into this sort of mess when we went in there.
# May 27, 1964
#![A picture of US soldiers carrying weapons in the farmland in Vietnam]
# The Vietnam War, also known as the Second Indochina War, and in Vietnam as the Resistance War Against America or simply the American War, was a conflict that occurred in Vietnam, Laos, and Cambodia from 1 November 1955 to the fall of Saigon on 30 April 1975. It was the second of the Indochina Wars and was officially fought between North Vietnam and the government of South Vietnam.

# Description of the dataset: the dataset describes all the air force operation in during the Vietnam War.
# The first dataset: Bombing_Operations dataset:
## AirCraft: Aircraft model (example: EC-47)
## ContryFlyingMission: Country
## MissionDate: Date of the mission
## OperationSupported: Supported War operation (example: Operation Rolling Thunder)
## PeriodOfDay: Day or night
## TakeoffLocation: Take off airport
## TimeOnTarget
## WeaponType
## WeaponsLoadedWeight

# The second dataset: Aircraft_Glossary dataset:
## AirCraft: Aircraft model (example: EC-47)
## AirCraftName
## AirCraftType

# Dataset Information:
# THOR is a painstakingly cultivated database of historic aerial bombings from World War I through Vietnam. THOR has already proven useful in finding unexploded ordnance in Southeast Asia and improving Air Force combat tactics: https://www.kaggle.com/usaf/vietnam-war-bombing-operations

## Step 3: Load the datasets
Bombing_Operations = spark.read.json("./data/Bombing_Operations.json.gz")
Aircraft_Glossary = spark.read.json("./data/Aircraft_Glossary.json.gz")

## Step 4: check the schema. (check logging)
bo = Bombing_Operations._jdf.schema().treeString()
ag = Aircraft_Glossary._jdf.schema().treeString()
logger.info("Step 4: {}".format(bo))
logger.info("Step 4: {}".format(ag))
# You can also show the schema by Bombing_Operations.printSchema() function but the output is not a string.

# Step 5: get a sample with take(). (check logging)
logger.info("Step 5: {}".format(Bombing_Operations.take(3)))

# Step 6: get a formatted sample in table format with a similar effect as take(). (check logging)
formatted_Aircraft_Glossary = Aircraft_Glossary.limit(10).toPandas()
logger.info("Step 6: ")
logger.info('\t'+ formatted_Aircraft_Glossary.to_string().replace('\n', '\n\t'))  # formatted_Aircraft_Glossary is the dataframe

# Step 7: get the number of rows in the dataset
Bombing_Operations.count()
logger.info("Step 7: In total there are {} operations".format(Bombing_Operations.count()))

# Housekeeping: Save dataset
Bombing_Operations.write.mode("overwrite").parquet("./data/Bombing_Operations.parquet")
Aircraft_Glossary.write.mode("overwrite").parquet("./data/Aircraft_Glossary.parquet")