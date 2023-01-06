# Questions 4: What is the most common category of job for someone working on an IMDB title in 1980?
# Keywords: join group by

## Housekeeping: setup code to read intermediate dataset
import pyspark, logging
from pyspark.sql import *
from pyspark.sql.functions import *
import pandas as pd

def script_filter(record):
    if record.name != __name__:
        return False
    return True
    
handler = logging.StreamHandler()
handler.filters = [script_filter]
logger = logging.getLogger(__name__)
logger.addHandler(handler)
fh = logging.FileHandler('./log/section_4.log', "w+")
logger.setLevel(logging.INFO)
logger.addHandler(fh)

spark = SparkSession.builder.config("spark.driver.memory", "4g").getOrCreate()

Titles = spark.read.csv("data/title.basics.tsv", sep='\t', header=True)
Principals = spark.read.csv("data/title.principals.tsv", sep='\t', header=True)
## End of Housekeeping.


# We begin by inspecting the content of `Principals`, which contains the principal cast and crew on each IMDB title:
logger.info("Step 1: inspecting the contents of 'Principals'")
logger.info('\t'+ pd.DataFrame(Principals.head(5)).to_string().replace('\n', '\n\t'))  # this step is a Pandas dataframe

# We are interested in the field `category`, which lists the category of job occupied by the cast or crew member.
logger.info("Step 2: inspecting the 'category' field")
logger.info('\t'+ pd.DataFrame(Principals.select("category").head(5)).to_string().replace('\n', '\n\t'))  # this step is a Pandas dataframe

# From the `Titles` dataframe, we first select all titles with `startYear` of 1980:
year1980_titles = Titles.where("startYear='1980'")

# We now join on the column `tconst` of both dataframes.
# With the Spark Dataframe API:
titles_joined = Principals.join(year1980_titles, Principals.tconst == year1980_titles.tconst)
logger.info("Step 3: inspecting the 'titles_joined' dataframe")
logger.info('\t'+ pd.DataFrame(titles_joined.head(5)).to_string().replace('\n', '\n\t'))  # this step is a Pandas dataframe

# We can select only the field we are interested in:
title_job_categories = titles_joined.select("category")

logger.info("Step 4: inspecting the 'title_job' dataframe")
logger.info('\t'+ pd.DataFrame(title_job_categories.head(5)).to_string().replace('\n', '\n\t'))  # this step is a Pandas dataframe

# And finally, we can group by `category` and then aggregate and count:
logger.info("Step 5: inspecting the result from grouping by 'category' and then aggregating and counting")
logger.info('\t'+ title_job_categories.groupBy("category").agg(count("*").alias("numPrincipals")).sort(desc("numPrincipals")).toPandas().to_string().replace('\n', '\n\t'))  # this step is a Pandas dataframe

# Alternatively, we can rewrite this query in pure SQL:
year1980_titles.registerTempTable("Year1980Titles")
Principals.registerTempTable("Principals")

query = """
SELECT category, count(*) numPrincipals
FROM Year1980Titles ti
JOIN Principals pr
ON ti.tconst = pr.tconst
GROUP BY category
ORDER BY numPrincipals DESC
"""

logger.info("Step 6: SQL query result")
logger.info('\t'+ spark.sql(query).toPandas().to_string().replace('\n', '\n\t'))  # this step is a Pandas dataframe

"""The job category of `actor` was most common in IMDB titles from 1980. The second most common job category was `actress`. Further data preprocessing might merge these two categories into a single non-gendered category."""