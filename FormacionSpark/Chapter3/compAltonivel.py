import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

#if __name__ == "__main__":
 #   if len(sys.argv) != 2:
  #      print("Usage: mnmcount <file>", file=sys.stderr)
   #     sys.exit(-1)

    # Build a SparkSession using the SparkSession APIs.
    # If one does not exist, then create an instance. There
    # can only be one SparkSession per JVM.

spark = (SparkSession
             .builder
             .appName("AuthorsAges")
             .getOrCreate())
# Create a DataFrame
data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30),("TD", 35), ("Brooke", 25)], ["name", "age"])
# Group the same names together, aggregate their ages, and compute an average
avg_df = data_df.groupBy("name").agg(avg("age"))
# Show the results of the final execution
avg_df.show()
# Stop the SparkSession
spark.stop()