# =============================================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# About this Code: script to move data from stage area to data warehouse into Hive
# =============================================================================================
# Version 1: creating the code
# =============================================================================================

%sql
-- Loading data to DIM_STORE
SELECT
    d.merchant_id                                       as  merchant_id
,   NVL(d.merchant_display_name, 'NA')                  as  merchant_display_name
,   NVL(d.merchant_name, 'NA')                          as  merchant_name
,   NVL(d.merchant_price, 0.0)                          as  merchant_price
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