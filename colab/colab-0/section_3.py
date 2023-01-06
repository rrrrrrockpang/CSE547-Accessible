# Question 3: Who bombed this location?
# Keywords: RDD map reduce cache save results

## Housekeeping: setup code to read intermediate dataset
import pyspark, logging
from pyspark.sql import *
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import time
import pandas as pd

def script_filter(record):
    if record.name != __name__:
        return False
    return True
    
handler = logging.StreamHandler()
handler.filters = [script_filter]
logger = logging.getLogger(__name__)
logger.addHandler(handler)
fh = logging.FileHandler('./log/section_3.log', "w+")
logger.setLevel(logging.INFO)
logger.addHandler(fh)

spark = SparkSession.builder.config("spark.driver.memory", "4g").getOrCreate()

Titles = spark.read.csv("data/title.basics.tsv", sep='\t', header=True)
Principals = spark.read.csv("data/title.principals.tsv", sep='\t', header=True)
## End of Housekeeping


# We are interested in discovering what genre of movie was the most common  during the year of 2000.

# We begin by filtering for movies that start in 2000:
year2000_movies = Titles.where("titleType='movie' AND startYear='2000'")

# We now split the genres column, which is currently formatted as a string, into an array of strings, then explode -- which means that we expand each row in the existing DataFrame by creating a copy of it for each distinct value in that array."""
year2000_movies = year2000_movies.select(["*", explode(split(Titles.genres, '[,]', 3)).alias("genre")])

logger.info("Step 1: movies that start in 2000")
logger.info('\t'+ pd.DataFrame(year2000_movies.head(20)).to_string().replace('\n', '\n\t'))  # this step is a Pandas dataframe

logger.info("Step 2: number of titles per genre2000")
logger.info('\t'+year2000_movies.groupBy("genre").agg(count("*").alias("numTitles")).sort(desc("numTitles")).toPandas().to_string().replace('\n', '\n\t'))  # this step is a Pandas dataframe

# 'Drama' was the most common genre in IMDB movies of 2000, with 1825 movies being listed as dramas.

# We can cache the content in memory, which saves time for repeat operations:

year2000_movies.cache()
# this step displays confirmation that the content was cached

# Now we count the number of rows and move the content to the cache:"""

# Commented out IPython magic to ensure Python compatibility.
# %time year2000_movies.count()
# this step displays the following information: CPU times: user 154 ms, sys: 28.9 ms, total: 183 ms, Wall time: 29.2 s 7049

# The second time we call `.count()`, the content has been cached and the operation is much faster:"""

# Commented out IPython magic to ensure Python compatibility.
# %time year2000_movies.count()
# this step displays the following information CPU times: user 4.4 ms, sys: 710 µs, total: 5.11 ms, Wall time: 123 ms, 7049

# We can also save the results in a file:
year2000_movies.write.mode('overwrite').json("./data/year2000_movies.json")

# We can then read from that file into a Spark DataFrame again.
year2000_movies = spark.read.json("./data/year2000_movies.json")

# As a reminder, we were previously using the Spark DataFrame API, as below:
Counts = year2000_movies.groupBy("genre").agg(count("*").alias("numTitles")).sort(desc("numTitles"))

logger.info("Step 3: number of titles of movies that start in 2000")
logger.info('\t'+ Counts.toPandas().to_string().replace('\n', '\n\t'))  # this step is a Pandas dataframe

# But we can also do the same operations using the explicit Map-Reduce format with RDDs.

# First we output, from each row, a tuple of data with format (genre, 1):
all_genres = year2000_movies.rdd.map(lambda row: (row.genre, 1))
all_genres.take(3)

# Then, we sum the counters in the reduce step, and we sort the resulting data by count:"""
genres_counts_rdd = all_genres.reduceByKey(lambda a, b: a+b).sortBy(lambda r: -r[1])

logger.info("Step 4: tuple of data with format (genre, 1)")
logger.info('\t'+ str(genres_counts_rdd.take(3)).replace('\n', '\n\t'))  # this step is a list of tuples

# Now we can convert the RDD to a DataFrame by mapping the pairs to objects of type `Row`
genres_counts_with_schema = genres_counts_rdd.map(lambda r: Row(genre=r[0], numTitles=r[1]))
genres_counts = spark.createDataFrame(genres_counts_with_schema)
logger.info("Step 4: dataframe where each row is of the format (genre, <sum>)")
logger.info('\t'+ genres_counts.toPandas().to_string().replace('\n', '\n\t'))  # this step is a Pandas dataframe


# As we see, the results with Map-Reduce match the results we had previously when conducting this query with the DataFrame API.