# Questions 4: What is the most used aircraft type during the Vietnam war (number of missions)?
# Keywords: join group by

## Housekeeping: setup code to read intermediate dataset
import pyspark, logging
from pyspark.sql import *
from pyspark.sql.functions import *

logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', 
                    level=logging.INFO, 
                    handlers=[
                            logging.FileHandler("./log/section_4.log", "w+"),
                            logging.StreamHandler()
                    ]
)

spark = SparkSession.builder.getOrCreate()

Aircraft_Glossary = spark.read.load("./data/Aircraft_Glossary.parquet")
Bombing_Operations = spark.read.load("./data/Bombing_Operations.parquet")
## End of Housekeeping.

# Step 1: Let's check the content of Aircraft_Glossary:
# We are interested in the filed AirCraftType here
formatted_Aircraft_Glossary = Aircraft_Glossary.limit(10).toPandas()
logging.info("Step 1: ")
logging.info('\t'+ formatted_Aircraft_Glossary.to_string().replace('\n', '\n\t')) 

# Step 2: We can join on the column AirCraft of both dataframes: the Aircraft_Glossary and Bombing_Operations.
# With Dataframe API:
missions_joined = Bombing_Operations.join(Aircraft_Glossary, 
                                          Bombing_Operations.AirCraft == Aircraft_Glossary.AirCraft)
logging.info("Step 2: ")
logging.info(missions_joined)

# Step 3: We can select only the field we are interested in:
missions_aircrafts = missions_joined.select("AirCraftType")
logging.info("Step 3: ")
logging.info('\t'+ missions_aircrafts.limit(10).toPandas().to_string().replace('\n', '\n\t')) 

# Step 4: And finally we can group by AirCraftType and count:
final_dataframe = missions_aircrafts.groupBy("AirCraftType").agg(count("*").alias("MissionsCount"))\
                  .sort(desc("MissionsCount"))
logging.info("Step 4: ")
logging.info('\t'+ final_dataframe.toPandas().to_string().replace('\n', '\n\t')) 

# Step 4: In alternative we can rewrite this in pure SQL:
Bombing_Operations.registerTempTable("Bombing_Operations")
Aircraft_Glossary.registerTempTable("Aircraft_Glossary")

query = """
SELECT AirCraftType, count(*) MissionsCount
FROM Bombing_Operations bo
JOIN Aircraft_Glossary ag
ON bo.AirCraft = ag.AirCraft
GROUP BY AirCraftType
ORDER BY MissionsCount DESC
"""
logging.info("Step 4: in alternative we can rewrite this in pure SQL: ")
logging.info('\t'+ spark.sql(query).limit(10).toPandas().to_string().replace('\n', '\n\t'))

# Takeaway: The aircrafts of type Fighter Jet Bomber participated in most of the missions in the Vietnam war.
# Note: This dataset would require further cleaning and normalization. See Fighter Jet Bomber, Jet Fighter Bomber, Fighter bomber jet