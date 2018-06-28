# =============================================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# About this Code:	extract all data from table "order_details" based on filter "merchant_id"
# 					and save the results in CSV file. 
# =============================================================================================
# Version 1: creating the code
# Version 2: query changed
# =============================================================================================

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext

sparkSession = (SparkSession
                .builder
                .appName('wishhack')
                .enableHiveSupport()
                .getOrCreate()
                )

strSQL = """
select
    distinct user_id
from
    wishhack.orderdetails_full_23062018_stg
"""


# Read from Hive
df = sparkSession.sql(strSQL)
df.show()


# Write into Hive
df.write.saveAsTable('DW_WISHHACK.USER')


# Save to CSV file
df.write.csv('user.csv')