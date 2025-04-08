from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

# Create a Spark session
spark = (SparkSession.builder.appName("demo").getOrCreate())

# Define the schema corresponding to the data
schema = StructType( [
StructField("user_id", IntegerType(), True),
StructField("kit_id", IntegerType(), True),
StructField("login_date", StringType(), True),
StructField("sessions_count", IntegerType(), True)])

# Data to be loaded into DataFrame
data = [
(1, 2, "2016-03-01", 5),
(1, 2, "2016-05-02", 6),
(2, 3, "2017-06-25", 1),
(3, 1, "2016-03-02", 0),
(3, 4, "2018-07-03", 5)
]

# Create DataFrame
inputDF = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
inputDF.show()

# write a query to determine the earliest login date for each user
from pyspark.sql.functions import rank
from pyspark.sql.window import Window
windowSpec = Window.partitionBy("user_id").orderBy("login_date")

rankedDF = inputDF.withColumn("rnk", rank().over(windowSpec))
rankedDF.show()

# resultDF = rankedDF.filter(rankedDF.rnk == 1).drop("rnk")
resultDF = rankedDF.select(f.col("user_id"), f.col("login_date").alias("first_login_date"), f.col("rnk")).filter(f.col("rnk") == 1)
resultDF.show()

from pyspark.sql.functions import rank, sum, min, lead
from pyspark.sql.window import Window
windowSpec = Window.partitionBy("user_id").orderBy("login_date")

rankedDF = inputDF.withColumn("rnk", rank().over(windowSpec))
rankedDF.show()

# resultDF = rankedDF.filter(rankedDF.rnk == 1).drop("rnk")
resultDF = rankedDF.select(f.col("user_id"), f.col("kit_id").alias("first_kit_used_by_player"), f.col("rnk")).filter(f.col("rnk") == 1)
resultDF.show()

windowSpec = Window.partitionBy("user_id").orderBy("login_date")

# write a solution to report for each player and date, how many games played so far by the player. that is, the total number of games played by the player until that date

runningDF = inputDF.withColumn("games_played_so_far", sum("sessions_count").over(windowSpec))

runningDF.select(f.col("user_id").alias("player_id"), f.col("login_date").alias("date"), f.col("games_played_so_far")).show()

from datetime import date
from pyspark.sql.functions import datediff, lead, col, min, when, to_date, lit
# windowSpec = Window.partitionBy("user_id").orderBy("login_date")

# write a solution to report the second consucutive login date for each player

leadDF = inputDF.withColumn("lead_date", lead("login_date").over(windowSpec))
leadDF.select(f.col("user_id").alias("players_id")).filter(f.col("lead_date").isNotNull()).show()

# another approach
# we need change the values in order to get conscutive login date

updatedDF = inputDF.withColumn("login_date", when(col("login_date") == to_date(lit("2016-05-02")), to_date(lit("2016-03-02"))).otherwise(col("login_date")))
updatedDF.show()
updatedDF.withColumn("first_login_date", f.min("login_date").over(Window.partitionBy("user_id")))\
                 .withColumn("lead_date", lead("login_date").over(Window.partitionBy("user_id").orderBy("login_date")))\
                 .filter(f.datediff("lead_date", "first_login_date") == 1).show()

# Now that leadDF1 is a DataFrame, we can continue
updatedDF.filter(f.col("sessions_count") > 5).show() # changed to refer to a column in the DataFrame

updatedDF.groupBy("user_id").agg(f.countDistinct("login_date").alias("total_logins")).show()

updatedDF.withColumn("first_login_date", f.min("login_date").over(Window.partitionBy("user_id"))).show()

updatedDF.withColumn("total_sesstions", sum("sessions_count").over(Window.partitionBy("user_id"))).show()

updatedDF=updatedDF.withColumn("user_first_login_date", min("login_date").over(Window.partitionBy("user_id")))


updatedDF= updatedDF.withColumn("is_first_login", when(updatedDF["login_date"] == updatedDF["user_first_login_date"], True).otherwise(False))
updatedDF.show()


multiple_logins_df = updatedDF.groupBy("user_id").agg(f.count_distinct("login_date").alias("total_logins"))
user_with_multiple_logins = multiple_logins_df.filter(multiple_logins_df.total_logins > 1)
user_with_multiple_logins.show()

updatedDF.withColumn("next_login_date", lead("login_date").over(Window.partitionBy("user_id").orderBy("login_date"))).show()

# from typing_extensions import final
no_of_days_df = updatedDF.withColumn("next_login_date", lead("login_date").over(Window.partitionBy("user_id").orderBy("login_date")))
no_of_days_df.show()
final_df = no_of_days_df.withColumn("no_of_days", datediff("next_login_date", "login_date"))
final_df.show()

from pyspark.sql.functions import when
updatedDF.withColumn("Sessions_count", when(f.col("sessions_count")==0,1).otherwise(f.col("sessions_count"))).show()

max_no_of_sessions_df = updatedDF.groupBy("user_id").agg(f.max("sessions_count").alias("max_sessions"))
max_no_of_sessions_df.show()
#  other way of writing
max_sessions_df = updatedDF.groupBy("user_id").agg(f.max("sessions_count").alias("max_sessions"))

# Join with the original DataFrame to get the corresponding login details
user_max_sessions_df = updatedDF.join(max_sessions_df, on="user_id", how="inner")

# Filter to keep only rows with maximum sessions
filtered_df = user_max_sessions_df.filter(user_max_sessions_df["sessions_count"] == user_max_sessions_df["max_sessions"])

# Show the result
filtered_df.select("user_id", "login_date", "sessions_count").show()