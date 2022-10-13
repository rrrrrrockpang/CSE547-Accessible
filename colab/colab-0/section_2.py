# Questions 2: Show the number of missions over time for each of the countries involved.
# Keywords: group by, parse date, plot
# check ./log/question_2.log for the output

## Housekeeping: setup code to read intermediate dataset
import pyspark, logging
from pyspark.sql import *
from pyspark.sql.functions import *
import matplotlib.pyplot as plt

logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', 
                    level=logging.INFO, 
                    handlers=[
                            logging.FileHandler("./log/section_2.log", "w+"),
                            logging.StreamHandler()
                    ]
)

spark = SparkSession.builder.getOrCreate()

Bombing_Operations = spark.read.load("./data/Bombing_Operations.parquet")
## End of Housekeeping

# Step 1: Let's select the relevant columns:
missions_countries = Bombing_Operations.selectExpr(["to_date(MissionDate) as MissionDate", "ContryFlyingMission"])
logging.info("Step 1: Let's select the relevant columns: {}".format(missions_countries))

# The filed MissionDate is converted to a Python date object.
# Step 2: Now we can group by MissionDate and ContryFlyingMission to get the count:
missions_by_date = missions_countries\
                    .groupBy(["MissionDate", "ContryFlyingMission"])\
                    .agg(count("*").alias("MissionsCount"))\
                    .sort(asc("MissionDate")).toPandas()
logging.info("Step 2: Now we can group by MissionDate and ContryFlyingMission to get the count:")
logging.info('\t'+ missions_by_date.head().to_string().replace('\n', '\n\t'))  # missions_by_date.head() is a Pandas dataframe

# Step 3: Now we can plot the content with a different series for each country:
fig = plt.figure(figsize=(10, 6))

## iterate the different groups to create a different series
for country, missions in missions_by_date.groupby("ContryFlyingMission"): 
    plt.plot(missions["MissionDate"], missions["MissionsCount"], label=country)

plt.legend(loc='best')
# Alt text: The resulting graph shows the time series of mission counts by day as line plot ranging from 1966-1975 for Australia, Korea (South), Laos, United States of America, Vietnam (South)
# The US has the most missions overall starting around 500 in 1966, reaching the peak over 3700 in mid 1968, decline to roughly around 1500 mid 1969, and increase again to around 3000 in 1970, and decline to below 500 late 1973. 
# Other countries remain low below 500. Vietnam (South) had a noticeable increase in 1970.
# Takeaway: We can observe how South Vietnam increased its missions starting from 1970. The drop in 1973 is motivated by the Paris Peace Accords that took place on January 27th, 1973, to establish peace in Vietnam and end the war.