# =============================================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# About this Code: create and populate the dimension table BIGTABLE_SALES
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
    ,   NVL(g.transaction_ip_address, 'NA')                 as  transaction_ip_address
    ,   NVL(g.transaction_user_info.email, 'NA')            as  transaction_email
    ,   NVL(g.transaction_user_info.login_method, 'NA')     as  transaction_login_method
    ,   NVL(g.transaction_user_info.verified, false)        as  transaction_verified
    ,   NVL(g.mtid, 'NA')                                   as  mtid
    ,   NVL(g.shipping_details.city, 'NA')                  as  shipping_city
    ,   NVL(g.shipping_details.country, 'NA')               as  shipping_country
    ,   NVL(g.shipping_details.country_code, 'NA')          as  shipping_country_code
    ,   NVL(g.shipping_details.name, 'NA')                  as  shipping_name
    ,   NVL(g.shipping_details.phone_number, 'NA')          as  shipping_phone_number
    ,   NVL(g.shipping_details.state, 'NA')                 as  shipping_state
    ,   NVL(g.shipping_details.state_abbreviation, 'NA')    as  shipping_state_abbreviation
    ,   NVL(g.shipping_details.street_address1, 'NA')       as  shipping_address1
    ,   NVL(g.shipping_details.street_address2, 'NA')       as  shipping_address2
    ,   NVL(g.shipping_details.zipcode, 'NA')               as  shipping_zipcode
    ,   NVL(g.time, CURRENT_TIMESTAMP)                      as  transaction_time
    ,   NVL(g.subtotal, 0.0)                                as  transaction_subtotal
    ,   NVL(g.total, 0.0)                                   as  transaction_total
    ,   g.user_id                                           as  user_id
    ,   d.product_id                                        as  product_id
    ,   NVL(d.image_url, 'NA')                              as  image_url
    ,   NVL(d.is_wish_express, false)                       as  is_wish_express
    ,   NVL(d.merchant_display_name, 'NA')                  as  merchant_display_name
    ,   d.merchant_id                                       as  merchant_id
    ,   NVL(d.merchant_name, 'NA')                          as  merchant_name
    ,   NVL(d.merchant_price, 0.0)                          as  merchant_price
    ,   NVL(d.name, 'NA')                                   as  product_name
    ,   NVL(d.original_price, 0.0)                          as  original_price
    ,   NVL(d.original_shipping, 0.0)                       as  original_shipping
    ,   NVL(d.quantity, 0)                                  as  quantity
    ,   NVL(d.shipped_date, CURRENT_DATE)                   as  shipped_date
    ,   NVL(d.size, 'NA')                                   as  size
    ,   d.variation_id                                      as  variation_id
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
df.write.saveAsTable('DW_WISHHACK.BIGTABLE_SALES')
