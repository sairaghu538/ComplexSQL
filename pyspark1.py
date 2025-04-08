from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("demo").getOrCreate()

data = [(1, "Laptop", "2024-08-01"),(1, "Mouse", "2024-08-05"),(1, "Keyboard", "2024-08-02"),(2, "Monitor", "2024-08-03")]
columns =["customer_id", "product", "purchase_date"]

df = spark.createDataFrame(data, columns)

df.show()

# window specification
window_spec = Window.partitionBy("customer_id").orderBy(col("purchase_date").desc())

# add row number column
deduplicated_df = df.withColumn("row_number", row_number().over(window_spec))\
                    .filter(col("row_number")==1)\
                    .drop("row_number")

# show the result
deduplicated_df.show()