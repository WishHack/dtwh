-- MY OWN PRODUCTS
-- TOP 10 COUNTRIES (MY OWN PRODUCTS)
SELECT shipping_country_code AS Country,
       cast(count(shipping_country_code) AS int) AS Ranking
FROM dw_wishhack.merchant
WHERE product_id in (
      '58b3fbfce4244b50fc5aa3b0'
    , '58b3d88eb3b76650500cd12b'
    , '5861dd67e62d6a4d4b785235'
    , '5a1bf45cff7c733adf34d75c'
    , '58219ce296585e3bc94ee1ea'
    , '5899846a8ec9f85d44cf73fa'
    , '59fda14e061be93b9576547a'
    , '59eb327e63d48401e9a909b7'
    , '567b6d5de7531a4bc2baa5c5'
    , '5684a412f75c3d46462c1a49')
GROUP BY shipping_country_code
ORDER BY Ranking DESC
LIMIT 10

-- BASED ON TOP 10 OWN PRODUCTS
-- WHO ARE THE CUSTOMERS? PRODUCT X COUNTRY (MORE REVENUE)
SELECT user_id AS UserID,
       sum(original_price+original_shipping) AS Revenue
FROM dw_wishhack.merchant
WHERE product_id in (
      '58b3fbfce4244b50fc5aa3b0'
    , '58b3d88eb3b76650500cd12b'
    , '5861dd67e62d6a4d4b785235'
    , '5a1bf45cff7c733adf34d75c'
    , '58219ce296585e3bc94ee1ea'
    , '5899846a8ec9f85d44cf73fa'
    , '59fda14e061be93b9576547a'
    , '59eb327e63d48401e9a909b7'
    , '567b6d5de7531a4bc2baa5c5'
    , '5684a412f75c3d46462c1a49')
    AND shipping_country_code in (
      'US'
    , 'FR'
    , 'GB'
    , 'DE'
    , 'BR'
    , 'IT'
    , 'SE'
    , 'AU'
    , 'CH'
    , 'DK'
        )
GROUP BY user_id
ORDER BY Revenue DESC
LIMIT 100

-- BASED ON TOP 10 OWN PRODUCTS
-- WHO ARE THE CUSTOMERS? PRODUCT X COUNTRY (MORE ORDERS)
SELECT user_id AS UserID,
       sum(quantity) AS QtyOrders
FROM dw_wishhack.merchant
WHERE product_id in (
      '58b3fbfce4244b50fc5aa3b0'
    , '58b3d88eb3b76650500cd12b'
    , '5861dd67e62d6a4d4b785235'
    , '5a1bf45cff7c733adf34d75c'
    , '58219ce296585e3bc94ee1ea'
    , '5899846a8ec9f85d44cf73fa'
    , '59fda14e061be93b9576547a'
    , '59eb327e63d48401e9a909b7'
    , '567b6d5de7531a4bc2baa5c5'
    , '5684a412f75c3d46462c1a49')
    AND shipping_country_code in (
      'US'
    , 'FR'
    , 'GB'
    , 'DE'
    , 'BR'
    , 'IT'
    , 'SE'
    , 'AU'
    , 'CH'
    , 'DK'
        )
GROUP BY user_id
ORDER BY QtyOrders DESC
LIMIT 100

-- BASED ON TOP 10 OWN PRODUCTS
-- WHO ARE THE CUSTOMERS? PRODUCT X COUNTRY (THEY DID A GOOD REVIEW)
SELECT 
    dwm.user_id AS UserID
    , dwr.rating AS Rating
FROM dw_wishhack.merchant dwm 
    JOIN (
        SELECT 
            whr.user_id
            , whr.product_id
            , whr.rating
        FROM wishhack.wishreview_stg whr
        ) AS dwr
    ON dwm.user_id = dwr.user_id
    AND dwm.product_id = dwr.product_id
WHERE dwm.product_id in (
      '58b3fbfce4244b50fc5aa3b0'
    , '58b3d88eb3b76650500cd12b'
    , '5861dd67e62d6a4d4b785235'
    , '5a1bf45cff7c733adf34d75c'
    , '58219ce296585e3bc94ee1ea'
    , '5899846a8ec9f85d44cf73fa'
    , '59fda14e061be93b9576547a'
    , '59eb327e63d48401e9a909b7'
    , '567b6d5de7531a4bc2baa5c5'
    , '5684a412f75c3d46462c1a49')
    AND dwm.shipping_country_code in (
      'US'
    , 'FR'
    , 'GB'
    , 'DE'
    , 'BR'
    , 'IT'
    , 'SE'
    , 'AU'
    , 'CH'
    , 'DK'
        )
    AND cast(dwr.rating AS int) >= 4
GROUP BY dwm.user_id, dwr.rating
ORDER BY dwr.rating DESC
LIMIT 100

-- BASED ON TOP 10 OWN PRODUCTS
-- WHO ARE THE CUSTOMERS? PRODUCT X COUNTRY (THEY DID A BAD REVIEW)
SELECT 
    dwm.user_id AS UserID
    , dwr.rating AS Rating
FROM dw_wishhack.merchant dwm 
    JOIN (
        SELECT 
            whr.user_id
            , whr.product_id
            , whr.rating
        FROM wishhack.wishreview_stg whr
        ) AS dwr
    ON dwm.user_id = dwr.user_id
    AND dwm.product_id = dwr.product_id
WHERE dwm.product_id in (
      '58b3fbfce4244b50fc5aa3b0'
    , '58b3d88eb3b76650500cd12b'
    , '5861dd67e62d6a4d4b785235'
    , '5a1bf45cff7c733adf34d75c'
    , '58219ce296585e3bc94ee1ea'
    , '5899846a8ec9f85d44cf73fa'
    , '59fda14e061be93b9576547a'
    , '59eb327e63d48401e9a909b7'
    , '567b6d5de7531a4bc2baa5c5'
    , '5684a412f75c3d46462c1a49')
    AND dwm.shipping_country_code in (
      'US'
    , 'FR'
    , 'GB'
    , 'DE'
    , 'BR'
    , 'IT'
    , 'SE'
    , 'AU'
    , 'CH'
    , 'DK'
        )
    AND cast(dwr.rating AS int) < 4
GROUP BY dwm.user_id, dwr.rating
ORDER BY dwr.rating DESC
LIMIT 100

-- BASED ON TOP 10 OWN PRODUCTS
-- TIME SERIES ANALYSIS - REVENUE DAILY
SELECT SUBSTRING(transaction_time, 1, 10) AS DtOrder ,
      SUM(original_price+original_shipping) AS Revenue
FROM dw_wishhack.merchant dwm
GROUP BY SUBSTRING(transaction_time, 1, 10)

-- BASED ON TOP 10 OWN PRODUCTS
-- TIME SERIES ANALYSIS - ORDERS DAILY
SELECT SUBSTRING(transaction_time, 1, 10) AS DtOrder ,
      SUM(quantity) AS QtyOrders
FROM dw_wishhack.merchant dwm
GROUP BY SUBSTRING(transaction_time, 1, 10)

-- BASED ON TOP 10 OWN PRODUCTS
-- OTHERS VENDORS
SELECT dwm.merchant_id AS MyStoreID,
       dwm.other_merchant_id AS OtherStoreID,
       dwm.merchant_display_name AS OtherStoreName,
       dwm.is_my_product AS IsMyProduct,
       sum(dwm.quantity) AS QtySold,
       sum(dwm.original_price + dwm.original_shipping) AS AmountSold
FROM dw_wishhack.merchant dwm
WHERE dwm.product_id in (
      '58b3fbfce4244b50fc5aa3b0'
    , '58b3d88eb3b76650500cd12b'
    , '5861dd67e62d6a4d4b785235'
    , '5a1bf45cff7c733adf34d75c'
    , '58219ce296585e3bc94ee1ea'
    , '5899846a8ec9f85d44cf73fa'
    , '59fda14e061be93b9576547a'
    , '59eb327e63d48401e9a909b7'
    , '567b6d5de7531a4bc2baa5c5'
    , '5684a412f75c3d46462c1a49')
    AND dwm.shipping_country_code in (
      'US'
    , 'FR'
    , 'GB'
    , 'DE'
    , 'BR'
    , 'IT'
    , 'SE'
    , 'AU'
    , 'CH'
    , 'DK'
        )
GROUP BY dwm.merchant_id,
         dwm.other_merchant_id,
         dwm.merchant_display_name,
         dwm.is_my_product
ORDER BY AmountSold DESC
LIMIT 100