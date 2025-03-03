#2 Using spark submit create a Dataframe and load the data from text file into the Dataframe. Use the first line as header.

from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, date_format, to_timestamp
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LoadTextFile") \
    .getOrCreate()
file_path= 'output1.txt'
# Load the data into a DataFrame
df = spark.read.option("header", "true").csv(file_path)
# Show the DataFrame contents
'''df.show()'''

#3) Remove leading and Trailing spaces from each field in the Dataframe and convert the timestamp to Month/Day/Year Hour:Minute:seconds format
df = df.select([trim(col(c)).alias(c) if c != "persistentCart" else col(c) for c in df.columns])
df = df.withColumn("Sys_date", date_format(col("timestamp"), "MM/dd/yyyy"))
df= df.withColumn("timestamp", date_format(to_timestamp("timestamp"), "MM/dd/yyyy HH:mm:ss"))
'''df.show()'''
#4) Create a new field called Sys_date.  The value of this field for each row should be the Date extracted from the column Timestamp in the Dataframe
df.show()
