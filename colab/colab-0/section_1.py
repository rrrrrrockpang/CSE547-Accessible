# Question 1: How many titles of each type are in the IMDB dataset?
# Keywords: Dataframe API, SQL, group by, sort
# check ./log/section_1.log for the output

## Housekeeping: setup code to read intermediate dataset
import pyspark, logging
from pyspark.sql import *
from pyspark.sql.functions import *

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

spark = SparkSession.builder.config("spark.driver.memory", "4g").getOrCreate()

Titles = spark.read.csv("data/title.basics.tsv", sep='\t', header=True)
Principals = spark.read.csv("data/title.principals.tsv", sep='\t', header=True)
## End of Housekeeping.

# Step 1: IMDB lists many different kinds of media -- movies, video games, TV episodes, etc. Let's group the IMDB titles by titleType and count how many records exist belonging to each title type:
title_type_counts = Titles.groupBy("titleType") \
    .agg(count("*").alias("numTitles")) \
    .sort(desc("numTitles"))

logger.info("Step 1: {}".format(title_type_counts))


# In this case we used the DataFrame API, but we could rewite the groupBy using pure SQL:
Titles.registerTempTable("Titles")

query = """
SELECT titleType, count(*) as numTitles
FROM Titles
GROUP BY titleType
ORDER BY numTitles DESC
"""

title_type_counts = spark.sql(query)

title_type_count_pd = title_type_counts.toPandas()

logger.info("Step 2: Check the title type counts dataframe. The Dataframe is small enough to be moved to Pandas:")
logger.info('\t'+ title_type_count_pd.head().to_string().replace('\n', '\n\t'))  # this step is a Pandas dataframe

# Step 3: We visualize this title type information by plotting a barchart with the number of titles by type:
pl = title_type_count_pd.plot(kind="bar",
                              x="titleType", y="numTitles",
                              figsize=(10, 7), log=True, alpha=0.5, color="olive")

pl.set_xlabel("Type of Title")
pl.set_ylabel("Number of Titles (Log scale)")
pl.set_title("Number of Titles by Type")
## Alt text: the resulting graph shows the number of titles by type on log scale
## From left to right, it shows tvEpisode with over 10^6, short with around 10^6, movie around 10^5.5,
# video and tvSeries a little over 10^5, tvMiniseries, tvSpecial and videoGame a bit below 10^5,
# tvShort a bit above 10^4 and tvPilot below 10^1