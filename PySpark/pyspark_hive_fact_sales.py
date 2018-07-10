# =============================================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# About this Code: create and populate the dimension table FACT_SALES
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
    SELECT
        g.transaction_id                                    as  transaction_id
    ,   g.user_id                                           as  user_id
    ,   CONCAT( SUBSTRING(g.time, 7, 4), 
                SUBSTRING(g.time, 1, 2), 
                SUBSTRING(g.time, 4, 2))                    as  time_id
    ,   d.product_id                                        as  product_id
    ,   d.merchant_id                                       as  merchant_id
    ,   d.variation_id                                      as  variation_id
    ,   NVL(g.mtid, 'NA')                                   as  mtid
    ,   NVL(d.original_price, 0.0)                          as  original_price
    ,   NVL(d.original_shipping, 0.0)                       as  original_shipping
    ,   NVL(d.quantity, 0)                                  as  quantity
    ,   NVL(g.time, CURRENT_TIMESTAMP)                      as  transaction_time
    ,   NVL(d.shipped_date, CURRENT_DATE)                   as  shipped_date
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
df.write.saveAsTable('DW_WISHHACK.FACT_SALES')
