-- OTHERS PRODUCTS
-- TOP 10 COUNTRIES (OTHERS PRODUCTS)
SELECT shipping_country_code AS Country,
       cast(count(shipping_country_code) AS int) AS Ranking
FROM dw_wishhack.merchant
WHERE product_id in (
      '58edf9a31f63ee191c842d74'
    , '59ba5cff77cc4e040c8111db'
    , '5928f7a74301d4609934cfaa'
    , '58d39a8bbee3656edb577e08'
    , '58c905d82185f8527edbc420'
    , '57c3f073a85b391f63a9f987'
    , '59e01ad3cd363737b5a7a7e2'
    , '55812d0c671bd919f8d12b02'
    , '57b99df0ea4b2c6d57728964'
    , '571872fefaf51a0866307888')
GROUP BY shipping_country_code
ORDER BY Ranking DESC
LIMIT 10

-- BASED ON TOP 10 OTHERS PRODUCTS
-- WHO ARE THE CUSTOMERS? PRODUCT X COUNTRY (MORE REVENUE)
SELECT user_id AS UserID,
	   sum(original_price+original_shipping) AS Revenue
FROM dw_wishhack.merchant
WHERE product_id in (
      '58edf9a31f63ee191c842d74'
    , '59ba5cff77cc4e040c8111db'
    , '5928f7a74301d4609934cfaa'
    , '58d39a8bbee3656edb577e08'
    , '58c905d82185f8527edbc420'
    , '57c3f073a85b391f63a9f987'
    , '59e01ad3cd363737b5a7a7e2'
    , '55812d0c671bd919f8d12b02'
    , '57b99df0ea4b2c6d57728964'
    , '571872fefaf51a0866307888')
    AND shipping_country_code in (
	  'US'
	, 'FR'
	, 'GB'
	, 'DE'
	, 'BR'
	, 'SE'
	, 'IT'
	, 'CH'
	, 'NO'
	, 'AU'
        )
GROUP BY user_id
ORDER BY Revenue DESC
LIMIT 100

-- BASED ON TOP 10 OTHERS PRODUCTS
-- WHO ARE THE CUSTOMERS? PRODUCT X COUNTRY (MORE ORDERS)
SELECT user_id AS UserID,
       sum(quantity) AS QtyOrders
FROM dw_wishhack.merchant
WHERE product_id in (
      '58edf9a31f63ee191c842d74'
    , '59ba5cff77cc4e040c8111db'
    , '5928f7a74301d4609934cfaa'
    , '58d39a8bbee3656edb577e08'
    , '58c905d82185f8527edbc420'
    , '57c3f073a85b391f63a9f987'
    , '59e01ad3cd363737b5a7a7e2'
    , '55812d0c671bd919f8d12b02'
    , '57b99df0ea4b2c6d57728964'
    , '571872fefaf51a0866307888')
    AND shipping_country_code in (
	  'US'
	, 'FR'
	, 'GB'
	, 'DE'
	, 'BR'
	, 'SE'
	, 'IT'
	, 'CH'
	, 'NO'
	, 'AU'
        )
GROUP BY user_id
ORDER BY QtyOrders DESC
LIMIT 100