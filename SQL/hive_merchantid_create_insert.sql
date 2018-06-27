# =============================================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# About this Code:  script to move data from stage area to data warehouse into Hive
# =============================================================================================
# Version 1: creating the code
# =============================================================================================

%sql
-- HIVE: DW_WISHHACK.MERCHANT
CREATE TABLE DW_WISHHACK.MERCHANT(
    merchant_id STRING,
    open_id STRING,
    transaction_id STRING,
    transaction_ip_address STRING,
    transaction_email STRING,
    transaction_login_method STRING,
    transaction_verified STRING,
    mtid STRING,
    shipping_city STRING,
    shipping_country STRING,
    shipping_country_code STRING,
    shipping_name STRING,
    shipping_phone_number STRING,
    shipping_state STRING,
    shipping_state_abbreviation STRING,
    shipping_address1 STRING,
    shipping_address2 STRING,
    shipping_zipcode STRING,
    transaction_time STRING,
    transaction_subtotal STRING,
    transaction_total STRING,
    user_id STRING,
    product_id STRING,
    image_url STRING,
    is_wish_express STRING,
    merchant_display_name STRING,
    merchant_name STRING,
    merchant_price STRING,
    product_name STRING,
    original_price STRING,
    original_shipping STRING,
    quantity STRING,
    shipped_date STRING,
    size STRING,
    variation_id STRING,
    is_my_product STRING
    )

%sql
-- Loading data to FACT_SALES
SET HIVE.EXEC.DYNAMIC.PARTITION.MODE=NOSTRICT;

-- APPEND MODE
INSERT INTO TABLE DW_WISHHACK.MERCHANT
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
    d.merchant_name as merchant_name,
    d.merchant_price as merchant_price,
    d.name as product_name,
    d.original_price as original_price,
    d.original_shipping as original_shipping,
    d.quantity as quantity,
    d.shipped_date as shipped_date,
    d.size as size,
    d.variation_id as variation_id,
    case when d.merchant_id = g.merchant_id
     then 1 else 0
     end as is_my_product
FROM
    wishhack.orderDetails_full_23062018_stg g
LATERAL VIEW
    explode(transaction_items) t as d
WHERE
    g.merchant_id = '56698779866093430fd111b0'