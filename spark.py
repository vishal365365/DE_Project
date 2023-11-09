from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lag,col,date_format,sum
import sys

# config variables used in spark app
host = sys.argv[sys.argv.index('--host')+1]
dbName = sys.argv[sys.argv.index('--db_name')+1]
table =  sys.argv[sys.argv.index('--table')+1]
user = sys.argv[sys.argv.index('--user')+1]
password = sys.argv[sys.argv.index('--pass')+1]
access_key = sys.argv[sys.argv.index('--access_key')+1]
secret_key = sys.argv[sys.argv.index('--secret_key')+1]
s3_output_location = sys.argv[sys.argv.index('--s3outputlocation')+1]

# creating spark session
spark = SparkSession.builder\
    .config("spark.hadoop.fs.s3a.access.key",access_key )\
    .config("spark.hadoop.fs.s3a.secret.key",secret_key )\
    .getOrCreate()
# reading data from RDS
df1 = spark.read.format("jdbc").options(driver = "com.mysql.cj.jdbc.Driver",\
                                        url = f"jdbc:mysql://{host}/{dbName}",\
                                        user = user,\
                                        password = password,\
                                        dbtable = table).load()
#creating window
window = Window.partitionBy("state_code").orderBy("date")
# adding required column for aggregration
df2 = df1.withColumn("newly_active",col('positive') - lag("positive",1,0).over(window)) \
      .withColumn("newly_cured",col("cured")-lag("cured",1,0).over(window)) \
      .withColumn("newly_death",col("death")-lag("death",1,0).over(window)) \
      .withColumn("year", date_format('date', "yyyy"))\
      .withColumn("month",date_format("date", "MM"))

# filtring the record as 2020-04-12 to 2020-07-31 as data is not avilable in between sequently 
# which will not create accurate result for the problem statement as month 05-2020 ,06-2020,07-2020
# are completley missing
# Problem statement : Get the monthly positivity, recovery and death rates due to covid
df3 = df2.filter("date >'2020-07-31'")
df4 = df3.groupBy("year","month").agg(sum('newly_active').alias("monthly_active"),\
      sum("newly_death").alias("monthly_death"),\
      sum("newly_cured").alias("monthly_cured"))
df5 = df4.select("year","month","monthly_active","monthly_death","monthly_cured").\
      orderBy("year","month")
df5.show()

# writing df2 to s3 in snappy compression partition on the basis of year month
df2.write.format("parquet")\
    .mode("overwrite")\
    .partitionBy("year","month")\
    .option("compression","snappy")\
    .save(s3_output_location)

