# =============================================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# About this Code: script to move data from stage area to data warehouse into Hive
# =============================================================================================
# Version 1: creating the code
# =============================================================================================

%sql
-- Loading data to DIM_PRODUCT
SELECT
    d.product_id                                        as  product_id
,   NVL(d.name, 'NA')                                   as  product_name
,   NVL(d.size, 'NA')                                   as  size
,   NVL(d.is_wish_express, false)                       as  is_wish_express
,   NVL(d.original_price, 0.0)                          as  original_price
,   NVL(d.original_shipping, 0.0)                       as  original_shipping
,   NVL(d.image_url, 'NA')                              as  image_url
FROM
    wishhack.orderDetails_full_23062018_stg g   
    LATERAL VIEW
    explode(transaction_items)  t   as  d
WHERE
        g.transaction_id    IS  NOT NULL
    AND g.user_id           IS  NOT NULL
    AND g.time              IS  NOT NULL    
    AND d.product_id        IS  NOT NULL
    AND d.merchant_id       IS  NOT NULL
    AND d.variation_id      IS  NOT NULL