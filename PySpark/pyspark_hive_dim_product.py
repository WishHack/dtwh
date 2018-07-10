# =============================================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# About this Code: create and populate the dimension table DIM_PRODUCT
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
        d.product_id                                        as  product_id
    ,   NVL(d.name, 'NA')                                   as  product_name
    ,   NVL(d.size, 'NA')                                   as  size
    ,   NVL(d.is_wish_express, false)                       as  is_wish_express
    ,   NVL(d.original_price, 0.0)                          as  original_price
    ,   NVL(d.original_shipping, 0.0)                       as  original_shipping
    ,   NVL(d.image_url, 'NA')                              as  image_url
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
df.write.saveAsTable('DW_WISHHACK.DIM_PRODUCT')
