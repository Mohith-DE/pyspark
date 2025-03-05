from pyspark.sql import SparkSession
from pyspark.sql.functions import hour,col, count,to_timestamp, collect_set, size,lag, unix_timestamp, min, max
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("parquet_df").getOrCreate()
df=spark.read.option('header','true')\
    .csv('data.csv')
#df.show()
5#)Write the output to a Parquet file and store it in HDFS.
hdfs_path= 'hdfs://localhost:9000/home/mohithd/tmpdata/dfs/name'
df.write.mode('overwrite').parquet(hdfs_path)
#print(f"Data written successfully to {hdfs_path}") 
#6) Create another Dataframe and load the data from the Parquet file in step 5.'''
df1=spark.read.parquet(hdfs_path)
#7) Find out the below insights by either running sql or dataframe functions on the Dataframe created in step6.
#a. Find out the total number of visits per each title
total_visits_per_title = df1.groupBy("title").agg(count("*").alias("total_visits"))
#total_visits_per_title.show()
#b) Find out the Hour of the Day with most visits overall
df_timestamp = df1.withColumn("timestamp", to_timestamp(col("timestamp"), "MM/dd/yyyy HH:mm:ss"))
hour_df = df_timestamp.withColumn("hour", hour(col("timestamp")))
top_hour_visits = hour_df.groupBy("hour").agg(count("*").alias("total_visits")).orderBy(col("total_visits").desc())
#top_hour_visits.show(1)
#c). Find out the User with most visits
most_visits = df1.groupBy("name").agg(count("*").alias("total_visits")).orderBy(col("total_visits").desc())
#most_visits.show(1)
#d). Find out the User with most visits for 'Remote Support: Geek Squad - Best Buy'
title_filtered = df1.filter("title = 'Remote Support: Geek Squad - Best Buy'")
most_visits_title = title_filtered.groupBy("name").agg(count("*").alias("total_visits")).orderBy(col("total_visits").desc())
#most_visits_title.show(1)
#e). Find out the number of users who has both 'Best Buy Support & Customer Service' and 'Remote Support: Geek Squad - Best Buy' 
titles_filtered = df1.filter("title = 'Remote Support: Geek Squad - Best Buy' OR title = 'Best Buy Support & Customer Service'")
titles_collected = titles_filtered.groupBy("name").agg(collect_set("title").alias("visited_titles"))
users_with_both = titles_collected.filter(size(col("visited_titles")) == 2)
user_count = users_with_both.count()
#print(f"Number of users who visited both pages: {user_count}")
#f). Find out the number of users who has both 'Best Buy Support & Customer Service' and 'Schedule a Service - Best Buy'
titles_filtered = df1.filter("title = 'Schedule a Service - Best Buy' OR title = 'Best Buy Support & Customer Service'")
titles_collected = titles_filtered.groupBy("name").agg(collect_set("title").alias("visited_titles"))
users_with_both = titles_collected.filter(size(col("visited_titles")) == 2)
user_count = users_with_both.count()
#print(f"Number of users who visited both pages: {user_count}")
#g). Find the User who has the longest time interval between visits. 
#h). Find the User with the shortest time interval between visits.
window_spec = Window.partitionBy("name").orderBy("timestamp")
# Get previous visit timestamp using lag
previous_visit = df_timestamp.withColumn("prev_timestamp", lag("timestamp").over(window_spec))
# Calculate time interval (in seconds) between visits
time_diff = previous_visit.withColumn("time_diff", unix_timestamp("timestamp") - unix_timestamp("prev_timestamp"))
# Remove null values (first visit per user has no previous timestamp)
time_diff_filtered = time_diff.filter(col("time_diff").isNotNull())
# Find the user with the longest time interval
longest_interval_user = time_diff_filtered.orderBy(col("time_diff").desc()).limit(1)
# Find the user with the shortest time interval
shortest_interval_user = time_diff_filtered.orderBy(col("time_diff").asc()).limit(1)
# Show results
print("User with the longest time interval:")
longest_interval_user.show()
print("User with the shortest time interval:")
shortest_interval_user.show()