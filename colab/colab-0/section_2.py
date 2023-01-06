# Questions 2: Show the number of missions over time for each of the countries involved.
# Keywords: group by, parse date, plot
# check ./log/section_2.log for the output

## Housekeeping: setup code to read intermediate dataset
import pyspark, logging
from pyspark.sql import *
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import pandas as pd

def script_filter(record):
    if record.name != __name__:
        return False
    return True
    
handler = logging.StreamHandler()
handler.filters = [script_filter]
logger = logging.getLogger(__name__)
logger.addHandler(handler)
fh = logging.FileHandler('./log/section_2.log', "w+")
logger.setLevel(logging.INFO)
logger.addHandler(fh)

spark = SparkSession.builder.config("spark.driver.memory", "4g").getOrCreate()

Titles = spark.read.csv("data/title.basics.tsv", sep='\t', header=True)
Principals = spark.read.csv("data/title.principals.tsv", sep='\t', header=True)
## End of Housekeeping

# Step 1: Let's select the relevant columns:
year_titles = Titles.selectExpr(["titleType", "startYear"])
logger.info("Step 1: Schema of year_titles: {}".format(str(year_titles.schema))) # this step is a schema

# The filed MissionDate is converted to a Python date object.
# Step 2: Now we can group by MissionDate and ContryFlyingMission to get the count:
titles_by_year_and_type = year_titles.groupBy(["titleType", "startYear"]).agg(count("*").alias("numTitles")).sort(asc("startYear")).toPandas()

logger.info("Step 2: We plot this data using a different series for each type of IMDB title:")
logger.info('\t'+ pd.DataFrame(titles_by_year_and_type.head()).to_string().replace('\n', '\n\t'))  # titles_by_year_and_type.head() is a Pandas dataframe

# Step 3: Now we can plot this data using a different series for each type of IMDB title:
fig = plt.figure(figsize=(10, 6))


# iterate the different groups to create a different series
for titleType, titles in titles_by_year_and_type.groupby("titleType"):

    plt.plot(
        pd.to_numeric(titles["startYear"][:-1]), titles["numTitles"][:-1], label=titleType
    )

plt.yscale('log')
plt.legend(loc='best')
# Alt text: The resulting graph shows the time series of title counts by day as line plot ranging from 1880-2022 for for movie, shorts, tvEpisode, tvMiniSeries, tvMOvie, tvPilog, tvSeries, tvShort, tvSpecial, video and videGame
# tvEpisode and has the most title counts overall, averaging over 10^4 titles after 1940 , reaching the peak of almost 10^6 near 2020.
# Other types remain low below 10^3 like tvShort, tvMiniseries and videGame.
# Takeaway: We now observe that titles of the 'short' and 'movie' types increase in count in the 1890s and 1900s. Other title types, which all depend on distribution via household TVs, increase in count much later, around the 1940s-1950s. T
# his intuitively coincides with TVs becoming more prevalent in households; in the US, household TV ownership rose from 9% to almost 90% during the 1950s.