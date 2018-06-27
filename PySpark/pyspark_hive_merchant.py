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
    SELECT    
        g.merchant_id as merchant_id,    
        g.openid as open_id,    
        g.transaction_id as transaction_id,
        g.transaction_ip_address as transaction_ip_address,
        g.transaction_user_info.email as transaction_email,
        g.transaction_user_info.login_method as transaction_login_method,    
        g.transaction_user_info.verified as transaction_verified,    
        g.mtid as mtid,
        g.shipping_details.city as shipping_city,
        g.shipping_details.country as shipping_country,
        g.shipping_details.country_code as shipping_country_code,
        g.shipping_details.name as shipping_name,
        g.shipping_details.phone_number as shipping_phone_number,
        g.shipping_details.state as shipping_state,
        g.shipping_details.state_abbreviation as shipping_state_abbreviation,
        g.shipping_details.street_address1 as shipping_address1,
        g.shipping_details.street_address2 as shipping_address2,
        g.shipping_details.zipcode as shipping_zipcode,    
        g.time as transaction_time,    
        g.subtotal as transaction_subtotal,
        g.total as transaction_total,    
        g.user_id as user_id,
        d.product_id as product_id,     
        d.image_url as image_url,
        d.is_wish_express as is_wish_express,     
        d.merchant_display_name as merchant_display_name,
        d.merchant_id as other_merchant_id,     
        d.merchant_name as merchant_name,     
        d.merchant_price as merchant_price,
        d.name as product_name,     
        d.original_price as original_price,
        d.original_shipping as original_shipping,     
        d.quantity as quantity,     
        d.shipped_date as shipped_date,
        d.size as size,     
        d.variation_id as variation_id,     
        case when d.merchant_id = g.merchant_id then 1 else 0 end as is_my_product
    FROM
        wishhack.orderDetails_full_23062018_stg g
        LATERAL VIEW
        explode(transaction_items) t as d
    WHERE    
        g.merchant_id = '56698779866093430fd111b0'

"""


# Read from Hive
df = sparkSession.sql(strSQL)
df.show()


# Write into Hive
df.write.saveAsTable('DW_WISHHACK.MERCHANT')


# Save to CSV file
df.write.csv('merchant.csv')