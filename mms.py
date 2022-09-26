import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: mmcount <file>", file=sys.stderr)
        sys.exit(-1)

    #First we build the spark session
    spark = (
        SparkSession
        .builder
        .appName("PythonMnMCount")
        .getOrCreate()
    )

    #Get the M&M file from the command line as an argument
    mnm_file = sys.argv[1]
    
    #Read the M&M file to an Spark DataFrame
    mnm_df = (spark.read.format("csv")
    .option("header", "true") #to set a header in the columns
    .option("inferSchema", "true") #to read the schema and give a name to each column based on data
    .load(mnm_file)
    )

    #1. Select from the DataFrame the fields "State", "Color", and "Count"
    #2. Group each state and its M&Ms color count
    #3. Aggregate counts of all colors and groupBy() State and Color
    #4. orderBy() in descending order

    count_mnm_df = (mnm_df
    .select("State", "Color", "Count")
    .groupBy("State", "Color")
    .agg(count("Count").alias("Total"))
    .orderBy("total", ascending = False)
    )

    #With show() we execute the query
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    #The last piece of code aggregated and counted for all the states
    #Now we are going to filter it by the state of CA
    ca_count_mnm_df = (mnm_df
    .select("State", "Color", "Count")
    .where(mnm_df.State == "CA")
    .groupBy("State", "Color")
    .agg(count("Count").alias("Total"))
    .orderBy("Total", ascending = False)
    )

    #With show() we execute the query
    ca_count_mnm_df.show(n=10, truncate=False)
    print("Total Rows = %d" % (ca_count_mnm_df.count()))

    #Stop the SparkSession
    spark.stop

