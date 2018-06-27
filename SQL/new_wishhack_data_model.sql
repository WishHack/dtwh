# =============================================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# About this Code:  script to move data from stage area to data warehouse into Hive
# =============================================================================================
# Version 1: creating the code
# =============================================================================================

-- Analysing the Customer's behavior: USER_VIEW_PAST, USER_VIEW_FUTURE, USER_VIEW_SOCIAL
-- HIVE: DW_WISHHACK.USER_VIEW_PAST
-- WISHUSER_STG + WISHREVIEW_STG
CREATE TABLE DW_WISHHACK.USER_VIEW_PAST (
	CustomerID STRING
	, CustomerRelation STRING
	, FacebookID STRING
	, CustomerFullName STRING
	, CustomerShortName STRING
	, CustomerLocale STRING
	, CustomerHasProfilePic STRING
	, CustomerIsWishStar STRING
	, CustomerIsFollowing STRING
	, CustomerIsFacebookUser STRING
	, CustomerIsTemporary STRING
	, ProductID STRING
	, ProductName STRING
	, ReviewRating STRING
	-- , ReviewDate STRING
	, ReviewUpvoteCount STRING
	, ReviewSizeChoice STRING
	-- , ReviewTime STRING
	, TransactionID STRING
	, CreatedAt STRING
	)
	-- PARTITIONED BY (CreatedAt_P DATE)
	-- STORED AS ORC;

INSERT INTO DW_WISHHACK.USER_VIEW_PAST
SELECT
	-- from WISHUSER_STG
	WHU.userid AS CustomerID
	, WHU.relation AS CustomerRelation
	, WHU.fb_uid AS FacebookID
	, WHU.name AS CustomerFullName
	, WHU.short_name AS CustomerShortName
	, WHU.locale AS CustomerLocale
	, WHU.has_profile_pic AS CustomerHasProfilePic
	, WHU.is_wish_star AS CustomerIsWishStar
	, WHU.is_following AS CustomerIsFollowing
	, WHU.is_fb_user AS CustomerIsFacebookUser
	, WHU.is_temporary AS CustomerIsTemporary
	-- from WISHREVIEW_STG
	, WHR.product_id AS ProductID
	, WHR.product_name AS ProductName
	, WHR.rating AS ReviewRating
	-- , WHR.date AS ReviewDate
	, WHR.upvote_count AS ReviewUpvoteCount
	, WHR.size_choice AS ReviewSizeChoice
	-- , WHR.time AS ReviewTime
	, WHR.transaction_id AS TransactionID
	, SUBSTRING(CURRENT_DATE, 1, 10) AS CreatedAt
FROM
	WISHHACK.WISHUSER_STG WHU
	INNER JOIN WISHHACK.WISHREVIEW_STG WHR
	ON WHU.userid = WHR.user_id

	INNER JOIN WISHHACK.WISHLIST_STG WHL
	ON WHU.userid = WHL.userid

-- HIVE: DW_WISHHACK.USER_VIEW_FUTURE
-- WISHUSER_STG + WISHLIST_STG + WISHLIST_STG_DETAIL
CREATE TABLE DW_WISHHACK.USER_VIEW_FUTURE (
	CustomerID STRING
	, CustomerRelation STRING
	, FacebookID STRING
	, CustomerFullName STRING
	, CustomerShortName STRING
	, CustomerLocale STRING
	, CustomerHasProfilePic STRING
	, CustomerIsWishStar STRING
	, CustomerIsFollowing STRING
	, CustomerIsFacebookUser STRING
	, CustomerIsTemporary STRING
	, WishlistID STRING
	, WishlistName STRING
	, WishlistPrivate STRING
	, WishlistPermalink STRING
	, WishlistSize STRING
	, DetailItemID STRING
	, DetailItemName STRING
	, DetailSellerPrice STRING
	, CategoryID STRING
	, CategoryName STRING
	, CategoryLevel STRING
	, CategoryIsParent STRING
	, CategoryParentID STRING
	, CreatedAt STRING
	)
	-- PARTITIONED BY (CreatedAt_P DATE)
	-- STORED AS ORC;

INSERT INTO DW_WISHHACK.USER_VIEW_FUTURE
SELECT
	-- from WISHUSER_STG
	WHU.userid AS CustomerID
	, WHU.relation AS CustomerRelation
	, WHU.fb_uid AS FacebookID
	, WHU.name AS CustomerFullName
	, WHU.short_name AS CustomerShortName
	, WHU.locale AS CustomerLocale
	, WHU.has_profile_pic AS CustomerHasProfilePic
	, WHU.is_wish_star AS CustomerIsWishStar
	, WHU.is_following AS CustomerIsFollowing
	, WHU.is_fb_user AS CustomerIsFacebookUser
	, WHU.is_temporary AS CustomerIsTemporary
	-- from WISHLIST_STG
	, WHL.bid AS WishlistID
	, WHL.name AS WishlistName
	, WHL.private AS WishlistPrivate 
	, WHL.permalink AS WishlistPermalink
	, WHL.size AS WishlistSize
	-- from WISHLIST_STG_DETAIL
	, WHLD.itemid AS DetailItemID
	, WHLD.itemname AS DetailItemName
	, WHLD.seller_price AS DetailSellerPrice
	-- from WISHCATEGORY
	, WHLC.cat_id AS CategoryID
	, WHLC.cat_name AS CategoryName
	, WHLC.cat_level AS CategoryLevel
	, WHLC.is_parent AS CategoryIsParent
	, WHLC.pcat_id AS CategoryParentID
	, SUBSTRING(CURRENT_DATE, 1, 10) AS CreatedAt
FROM
	WISHHACK.WISHUSER_STG WHU
	INNER JOIN WISHHACK.WISHLIST_STG WHL
	ON WHU.userid = WHL.userid

	INNER JOIN WISHHACK.WISHLIST_DETAIL_STG WHLD
	ON WHL.itemid = WHLD.itemid

	INNER JOIN WISHHACK.WISHCATEGORY WHLC
	ON WHLD.cat_ids = WHLC.cat_id

-- HIVE: DW_WISHHACK.USER_VIEW_SOCIAL
-- WISHUSER_STG
CREATE TABLE DW_WISHHACK.USER_VIEW_SOCIAL (
	CustomerID STRING
	, CustomerRelation STRING
	, FacebookID STRING
	, CustomerFullName STRING
	, CustomerShortName STRING
	, CustomerLocale STRING
	, CustomerHasProfilePic STRING
	, CustomerIsWishStar STRING
	, CustomerIsFollowing STRING
	, CustomerIsFacebookUser STRING
	, CustomerIsTemporary STRING
	, CreatedAt STRING
	)
	-- PARTITIONED BY (CreatedAt_P DATE)
	-- STORED AS ORC;

INSERT INTO DW_WISHHACK.USER_VIEW_SOCIAL
SELECT
	-- from WISHUSER_STG
	WHU.userid AS CustomerID
	, WHU.relation AS CustomerRelation
	, WHU.fb_uid AS FacebookID
	, WHU.name AS CustomerFullName
	, WHU.short_name AS CustomerShortName
	, WHU.locale AS CustomerLocale
	, WHU.has_profile_pic AS CustomerHasProfilePic
	, WHU.is_wish_star AS CustomerIsWishStar
	, WHU.is_following AS CustomerIsFollowing
	, WHU.is_fb_user AS CustomerIsFacebookUser
	, WHU.is_temporary AS CustomerIsTemporary
	, SUBSTRING(CURRENT_DATE, 1, 10) AS CreatedAt
FROM
	WISHHACK.WISHUSER_STG WHU
	INNER JOIN WISHHACK.WISHREVIEW_STG WHR
	ON WHU.userid = WHR.user_id

	INNER JOIN WISHHACK.WISHLIST_STG WHL
	ON WHU.userid = WHL.userid

-- Analysing the Merchant's sales: MERCHANT_VIEW_USERS, MERCHANT_VIEW_TRANSACTION, MERCHANT_VIEW_WISHLIST_STG, CLUSTER_USER_VIEW
-- HIVE: DW_WISHHACK.MERCHANT_VIEW_USERS
-- WISHUSER_STG + ORDER_DETAILS
CREATE TABLE DW_WISHHACK.MERCHANT_VIEW_USERS (
	CustomerID STRING
	, CustomerRelation STRING
	, FacebookID STRING
	, CustomerFullName STRING
	, CustomerShortName STRING
	, CustomerEmail STRING
	, CustomerLocale STRING
	, CustomerHasProfilePic STRING
	, CustomerIsWishStar STRING
	, CustomerIsFollowing STRING
	, CustomerIsFacebookUser STRING
	, CustomerIsTemporary STRING
	, MerchantID STRING
 	, TransactionID	STRING
	, ProductID STRING
	, ProductName STRING
	, ReviewRating STRING
	, ReviewDate STRING
	, ReviewUpvoteCount STRING
	, ReviewSizeChoice STRING
	, ReviewTime STRING
	, ReviewComment STRING
	, CreatedAt STRING
	)
	-- PARTITIONED BY (CreatedAt_P DATE)
	-- STORED AS ORC;

INSERT INTO DW_WISHHACK.MERCHANT_VIEW_USERS
SELECT
	-- from WISHUSER_STG
	WHU.userid AS CustomerID
	, WHU.relation AS CustomerRelation,
	, WHU.fb_uid AS FacebookID,
	, WHU.name AS CustomerFullName,
	, WHU.short_name AS CustomerShortName,
	, WHO.transaction_user_info.email AS CustomerEmail
	, WHU.locale AS CustomerLocale,
	, WHU.has_profile_pic AS CustomerHasProfilePic,
	, WHU.is_wish_star AS CustomerIsWishStar,
	, WHU.is_following AS CustomerIsFollowing,
	, WHU.is_fb_user AS CustomerIsFacebookUser,
	, WHU.is_temporary AS CustomerIsTemporary,
	-- from ORDERDETAILS
	, WHO.merchant_id AS MerchantID
 	, WHO.transaction_id AS TransactionID	
	-- from WISHREVIEW_STG
	, WHR.product_id AS ProductID
	, WHR.product_name AS ProductName
	, WHR.rating AS ReviewRating
	, WHR.date AS ReviewDate
	, WHR.upvote_count AS ReviewUpvoteCount
	, WHR.size_choice AS ReviewSizeChoice
	, WHR.time AS ReviewTime
	, WHR.comment AS ReviewComment
	, SUBSTRING(CURRENT_DATE, 1, 10) AS CreatedAt
FROM
	WISHHACK.WISHUSER_STG WHU
	INNER JOIN WISHHACK.ORDERDETAILS_TEST_STG WHO
	ON WHU.userid = WHO.user_id

	WISHHACK.ORDERDETAILS_TEST_STG WHO
	INNER JOIN WISHHACK.WISHREVIEW_STG WHR
	ON WHO.transaction_id = WHR.transaction_id
	AND WHO.user_id = WHR.user_id

-- HIVE: DW_WISHHACK.MERCHANT_VIEW_TRANSACTION
-- ORDER_DETAILS
CREATE TABLE DW_WISHHACK.MERCHANT_VIEW_TRANSACTION (
	, MerchantID STRING
 	, TransactionID	STRING
	, TransactionAppType STRING
	, TransactionAppName STRING
	, TransactionAppClient STRING
	, TransactionCurencyCode STRING
	, TransactionIsGift STRING
	, TransactionIsNewRefund STRING
	, TransactionMaxShipping STRING
	, TransactionMinShipping STRING
	, TransactionPaymentType STRING
	, TransactionPaymentNumber STRING
	, CreatedAt STRING
	)
	-- PARTITIONED BY (CreatedAt_P DATE)
	-- STORED AS ORC;

INSERT INTO DW_WISHHACK.MERCHANT_VIEW_TRANSACTION
SELECT
	, WHO.merchant_id AS MerchantID
 	, WHO.transaction_id AS TransactionID	
	, WHO.transaction_app_type AS TransactionAppType
	, WHO.transaction_app_type_name AS TransactionAppName
	, WHO.transaction_client AS TransactionAppClient
	, WHO.transaction_currency_code AS TransactionCurencyCode
	, WHO.transaction_is_gift AS TransactionIsGift
	, WHO.transaction_is_new_refund AS TransactionIsNewRefund
	, WHO.max_shipping_time AS TransactionMaxShipping
	, WHO.min_shipping_time AS TransactionMinShipping
	, WHO.payment_type AS TransactionPaymentType
	, WHO.payment_type_number AS TransactionPaymentNumber
	, SUBSTRING(CURRENT_DATE, 1, 10) AS CreatedAt
FROM
	WISHHACK.ORDERDETAILS_TEST_STG WHO

-- HIVE: DW_WISHHACK.MERCHANT_VIEW_WISHLIST_STG
-- WISHUSER_STG + WISHLIST_STG + ORDER_DETAILS
CREATE TABLE DW_WISHHACK.MERCHANT_VIEW_WISHLIST_STG (
	MerchantID STRING
	, CustomerID STRING
	, CustomerRelation STRING
	, FacebookID STRING
	, CustomerFullName STRING
	, CustomerShortName STRING
	, CustomerLocale STRING
	, CustomerHasProfilePic STRING
	, CustomerIsWishStar STRING
	, CustomerIsFollowing STRING
	, CustomerIsFacebookUser STRING
	, CustomerIsTemporary STRING
	, WishlistID STRING
	, WishlistName STRING
	, WishlistPrivate STRING
	, WishlistPermalink STRING
	, WishlistSize STRING
	, WishlistItemID STRING
	, WishlistItemName STRING
	, WishlistSellerPrice STRING
	, WishlistCategoryID STRING
	, CreatedAt STRING
	)
	-- PARTITIONED BY (CreatedAt_P DATE)
	-- STORED AS ORC;

INSERT INTO DW_WISHHACK.MERCHANT_VIEW_WISHLIST_STG
SELECT
	-- from ORDER_DETAILS
	WHO.merchant_id AS MerchantID
	-- from WISHUSER_STG
	, WHU.userid AS CustomerID
	, WHU.relation AS CustomerRelation
	, WHU.fb_uid AS FacebookID
	, WHU.name AS CustomerFullName
	, WHU.short_name AS CustomerShortName
	, WHU.locale AS CustomerLocale
	, WHU.has_profile_pic AS CustomerHasProfilePic
	, WHU.is_wish_star AS CustomerIsWishStar
	, WHU.is_following AS CustomerIsFollowing
	, WHU.is_fb_user AS CustomerIsFacebookUser
	, WHU.is_temporary AS CustomerIsTemporary
	-- from WISHLIST_STG
	, WHL.bid AS WISHLIST_STGID
	, WHL.name AS WISHLIST_STGName
	, WHL.private AS WISHLIST_STGPrivate
	, WHL.permalink AS WISHLIST_STGPermalink
	, WHL.size AS WISHLIST_STGSize
	-- from WISHLIST_STG_DETAIL
	, WHLD.itemid AS WISHLIST_STGItemID
	, WHLD.itemname AS WISHLIST_STGItemName
	, WHLD.seller_price AS WISHLIST_STGSellerPrice
	, WHLD.cat_ids AS WISHLIST_STGCategoryID
	, SUBSTRING(CURRENT_DATE, 1, 10) AS CreatedAt
FROM
	WISHHACK.ORDERDETAILS_TEST_STG WHO
	INNER JOIN WISHHACK.WISHUSER_STG WHU
	ON WHO.user_id = WHU.userid

	WISHHACK.WISHUSER_STG WHU
	INNER JOIN WISHHACK.WISHLIST_STG WHL
	ON WHU.userid = WHL.userid

	WISHHACK.WISHLIST_STG WHL
	INNER JOIN WISHHACK.WISHLIST_STG_DETAIL WHLD
	ON WHL.itemid = WHLD.itemid

-- HIVE: DW_WISHHACK.CLUSTER_USER_VIEW
-- WISHUSER_STG + WISHREVIEW_STG + ORDER
CREATE TABLE DW_WISHHACK.CLUSTER_USER_VIEW (
	, MerchantID STRING
	, CustomerID STRING
	, CustomerRelation STRING
	, FacebookID STRING
	, CustomerFullName STRING
	, CustomerShortName STRING
	, CustomerLocale STRING
	, CustomerHasProfilePic STRING
	, CustomerIsWishStar STRING
	, CustomerIsFollowing STRING
	, CustomerIsFacebookUser STRING
	, CustomerIsTemporary STRING
	, ProductID STRING
	, ProductName STRING
	, ReviewRating STRING
	, ReviewUpvoteCount STRING
	, ReviewSizeChoice STRING
	, SUBSTRING(CURRENT_DATE, 1, 10) AS CreatedAt
INSERT INTO DW_WISHHACK.CLUSTER_USER_VIEW
SELECT
	-- from ORDER_DETAILS
	WHO.merchant_id AS MerchantID
	-- from WISHUSER_STG
	, WHU.userid AS CustomerID
	, WHU.relation AS CustomerRelation
	, WHU.fb_uid AS FacebookID
	, WHU.name AS CustomerFullName
	, WHU.short_name AS CustomerShortName
	, WHU.locale AS CustomerLocale
	, WHU.has_profile_pic AS CustomerHasProfilePic
	, WHU.is_wish_star AS CustomerIsWishStar
	, WHU.is_following AS CustomerIsFollowing
	, WHU.is_fb_user AS CustomerIsFacebookUser
	, WHU.is_temporary AS CustomerIsTemporary
	-- from WISHREVIEW_STG
	, WHR.product_id AS ProductID
	, WHR.product_name AS ProductName
	, WHR.rating AS ReviewRating
	, WHR.upvote_count AS ReviewUpvoteCount
	, WHR.size_choice AS ReviewSizeChoice
	, SUBSTRING(CURRENT_DATE, 1, 10) AS CreatedAt
FROM
	WISHHACK.ORDERDETAILS_TEST_STG WHO
	INNER JOIN WISHHACK.WISHUSER_STG WHU
	ON WHO.user_id = WHU.userid
	
	INNER JOIN WISHHACK.WISHREVIEW_STG WHR
	ON WHO.transaction_id = WHR.transaction_id
	AND WHO.user_id = WHR.user_id
