# Question 1: Which countries are involved and in how many missions?
# Keywords: Dataframe API, SQL, group by, sort
# check ./log/question_1.log for the output

## Housekeeping: setup code to read intermediate dataset
import pyspark, logging
from pyspark.sql import *
from pyspark.sql.functions import *

logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', 
                    level=logging.INFO, 
                    handlers=[
                            logging.FileHandler("./log/section_1.log", "w+"),
                            logging.StreamHandler()
                    ]
)

spark = SparkSession.builder.getOrCreate()

Bombing_Operations = spark.read.load("./data/Bombing_Operations.parquet")
## End of Housekeeping.

# Step 1: Let's group the missions by ContryFlyingMission and count how many records exist:
missions_counts = Bombing_Operations.groupBy("ContryFlyingMission")\
                                    .agg(count("*").alias("MissionsCount"))\
                                    .sort(desc("MissionsCount"))

# In this case we used the DataFrame API, but we could rewite the groupBy using pure SQL:
Bombing_Operations.registerTempTable("Bombing_Operations")
query = """
SELECT ContryFlyingMission, count(*) as MissionsCount
FROM Bombing_Operations
GROUP BY ContryFlyingMission
ORDER BY MissionsCount DESC
"""
missions_counts = spark.sql(query)

# Step 2: Check the dataframe. The Dataframe is small enough to be moved to Pandas:
missions_count_pd = missions_counts.toPandas()
logging.info("Step 2: Check the dataframe. The Dataframe is small enough to be moved to Pandas:")
logging.info('\t'+ missions_count_pd.to_string().replace('\n', '\n\t'))  # missions_count_pd is a Pandas dataframe

# Step 3: Let's plot a barchart with the number of missions by country:
pl = missions_count_pd.plot(kind="bar", 
                            x="ContryFlyingMission", y="MissionsCount", 
                            figsize=(10, 7), log=True, alpha=0.5, color="olive")
pl.set_xlabel("Country")
pl.set_ylabel("Number of Missions (Log scale)")
pl.set_title("Number of missions by Country")
## Alt text: the resulting graph shows the number of missions by country on log scale
## From left to right, it shows United States of America with over 10^6, Vietnam (South) with around 10^6, Laos around 10^4.5, Korea (South) over 10^4, Australia a little over 10^4 but below Korea (South)