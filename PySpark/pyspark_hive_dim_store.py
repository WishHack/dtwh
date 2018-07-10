# =============================================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# About this Code: create and populate the dimension table DIM_STORE
# =============================================================================================
# Version 1: creating the code
# =============================================================================================

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext

sparkSession = (SparkSession
                .builder
                .appName('dw_wishhack')
                .enableHiveSupport()
                .getOrCreate()
                )

strSQL = """
    SELECT DISTINCT
        d.merchant_id                                       as  merchant_id
    ,   NVL(d.merchant_display_name, 'NA')                  as  merchant_display_name
    ,   NVL(d.merchant_name, 'NA')                          as  merchant_name
    FROM
        wishhack.orderDetails_full_23062018_stg g   LATERAL VIEW
        explode(transaction_items)  t   as  d
    WHERE
            g.transaction_id    IS  NOT NULL
        AND g.user_id           IS  NOT NULL
        AND g.time              IS  NOT NULL        
        AND d.product_id        IS  NOT NULL
        AND d.merchant_id       IS  NOT NULL
        AND d.variation_id      IS  NOT NULL
"""


# Read from Hive
df = sparkSession.sql(strSQL)


# Write into Hive
df.write.saveAsTable('DW_WISHHACK.DIM_STORE')
