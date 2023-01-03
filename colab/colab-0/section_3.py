# Question 3: Who bombed this location?
# Keywords: RDD map reduce cache save results

# Picture: This picture is the Hanoi POL facility (North Vietnam) burning after it was attacked by the U.S. Air Force on 29 June 1966 in the context of the Rolling Thunder operation.

## Housekeeping: setup code to read intermediate dataset
import pyspark, logging
from pyspark.sql import *
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import time

def script_filter(record):
    if record.name != __name__:
        return False
    return True
    
handler = logging.StreamHandler()
handler.filters = [script_filter]
logger = logging.getLogger(__name__)
logger.addHandler(handler)
fh = logging.FileHandler('./log/section_3.log')
logger.setLevel(logging.INFO)
logger.addHandler(fh)

spark = SparkSession.builder.getOrCreate()

Bombing_Operations = spark.read.load("./data/Bombing_Operations.parquet")
## End of Housekeeping

# We are interested in discovering what was the most common take-off location during that day.
jun_29_operations = Bombing_Operations.where("MissionDate = '1966-06-29' AND TargetCountry='NORTH VIETNAM'")

# Step 1: Which coutries scheduled missions that day?
step1 = jun_29_operations.groupBy("ContryFlyingMission").agg(count("*").alias("MissionsCount")).toPandas()
logger.info("Step 1: Which coutries scheduled missions that day?")
logger.info('\t'+ step1.to_string().replace('\n', '\n\t'))  # step1 is a Pandas dataframe

# Step 2: Check the jun_29_operations dataset
# Most of the operation that day were performed by USA airplanes.
logger.info(jun_29_operations.take(1))

# Step 3: You can specify to cache the content in memory:
jun_29_operations.cache()

# Now you can count the number of rows and move the content to the cache:
start_time = time.time()
uncached = jun_29_operations.count()
logger.info("Step 3: uncached time: {}".format(time.time() - start_time))

# The second time the content is cached and the operation is much faster:
start_time = time.time()
cached = jun_29_operations.count()
logger.info("Step 3: cached time: {}".format(time.time() - start_time))

# You can also save the results on a file...
jun_29_operations.write.mode('overwrite').json("./data/jun_29_operations.json")

# ... and read from the file:
jun_29_operations = spark.read.json("./data/jun_29_operations.json")

# Step 4: We can use the simple DataFrame API to count the number of operations by country
# and sort the results by the number of operations
TakeoffLocationCounts = jun_29_operations\
                            .groupBy("TakeoffLocation").agg(count("*").alias("MissionsCount"))\
                            .sort(desc("MissionsCount"))
formatted_TakeoffLocationCounts = TakeoffLocationCounts.limit(10).toPandas()
logger.info("Step 4: using simple DataFrame API: ")
logger.info('\t'+ formatted_TakeoffLocationCounts.to_string().replace('\n', '\n\t')) 

# ... or the explicit Map/Reduce format with RDDs.
# First we emit a pair in the format (Location, 1):
all_locations = jun_29_operations.rdd.map(lambda row: (row.TakeoffLocation, 1))
logger.info("Step 4: using Map/Reduce with RDDs: we emit a pair in the format (Location, 1)")
logger.info(all_locations.take(3))

# Then, we sum counters in the reduce step, and we sort by count:
locations_counts_rdd = all_locations.reduceByKey(lambda a, b: a+b).sortBy(lambda r: -r[1])
logger.info("Step 4: using Map/Reduce with RDDs: sum counters in the reduce step, and we sort by count:")
logger.info(locations_counts_rdd.take(3))

# Now we can convert the RDD in dataframe by mapping the pairs to objects of type Row
locations_counts_with_schema = locations_counts_rdd.map(lambda r: Row(TakeoffLocation=r[0], MissionsCount=r[1]))
locations_counts = spark.createDataFrame(locations_counts_with_schema)
logger.info("Step 4: using Map/Reduce with RDDs: convert the RDD in dataframe by mapping the pairs to objects of type Row: ")
logger.info('\t'+ locations_counts.toPandas().to_string().replace('\n', '\n\t'))  # locations_counts is a Spark dataframe, we converted it to pandas for better formatting

# Takeaway: That day the most common take-off location was the ship USS Constellation (CV-64). We cannot univocally identify one take off location, but we can reduce the possible candidates. Next steps: explore TimeOnTarget feature.
# USS Constellation (CV-64), a Kitty Hawk-class supercarrier, was the third ship of the United States Navy to be named in honor of the "new constellation of stars" on the flag of the United States. One of the fastest ships in the Navy, as proven by her victory during a battlegroup race held in 1985, she was nicknamed "Connie" by her crew and officially as "America's Flagship".