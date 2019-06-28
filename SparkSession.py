#import a SparkSession
from pyspark.sql import SparkSession
#create a sparkSession
spark=SparkSession.builder.appName("grouping").getOrCreate()
#read the file with header option true
df=spark.read.csv("custormer-orders.csv",inferSchema=True,header=True)
#see the results
df.show()
#group the data
grouped=df.groupBy("dept").count().orderBy("count",ascending=False)
#show grouped data
grouped.show()
#another way to show distinct data in descending order
df.select("genres").distinct().orderBy("genres",ascending=False).show()
