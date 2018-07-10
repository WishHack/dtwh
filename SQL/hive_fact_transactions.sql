# =============================================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# About this Code: script to move data from stage area to data warehouse into Hive
# =============================================================================================
# Version 1: creating the code
# =============================================================================================

%sql
-- Loading data to FACT_TRANSACTIONS
SELECT
        g.transaction_id                                                                    as  transaction_id
    ,   g.user_id                                                                           as  user_id
    ,   CONCAT(SUBSTRING(g.time, 7, 4), SUBSTRING(g.time, 1, 2), SUBSTRING(g.time, 4, 2))   as  time_id
    ,   d.merchant_id                                                                       as  merchant_id
    ,   d.variation_id                                                                      as  variation_id
    ,   NVL(g.mtid, 'NA')                                                                   as  mtid
    ,   SUM(NVL(d.original_price, 0.0))                                                     as  original_price
    ,   SUM(NVL(d.original_shipping, 0.0))                                                  as  original_shipping
    ,   SUM(NVL(d.original_price, 0.0)  +   NVL(d.original_shipping, 0.0))                  as  transaction_total
    ,   SUM(NVL(d.quantity, 0))                                                             as  quantity
    ,   NVL(g.time, CURRENT_TIMESTAMP)                                                      as  transaction_time
    ,   NVL(d.shipped_date, CURRENT_DATE)                                                   as  shipped_date
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
GROUP BY
        g.transaction_id
    ,   g.user_id
    ,   g.time
    ,   d.merchant_id
    ,   d.variation_id
    ,   g.mtid
    ,   d.shipped_date