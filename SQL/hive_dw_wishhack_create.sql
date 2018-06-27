# =============================================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# About this Code:  script to create database and all tables into Hive
# =============================================================================================
# Version 1: creating the code
# =============================================================================================

-- HIVE: DW WISHHACK
CREATE DATABASE IF NOT EXISTS DW_WISHHACK;

-- HIVE: DW_WISHHACK.DIM_CUSTOMER
CREATE TABLE DW_WISHHACK.DIM_CUSTOMER(
  CustomerID STRING
  , FullName STRING
  , ShortName STRING
  , Email STRING
  , City STRING
  , Country STRING
  , CountryCode STRING
  , PhoneNumber STRING
  , State STRING
  , StateAbbreviation STRING
  , StreetAddress1 STRING
  , StreetAddress2 STRING
  , Zipcode STRING
  , HasProfilePic STRING
  , IsWishStar STRING
  , IsFollowing STRING
  , IsFacebookUser STRING
  , IsTemporary STRING
  , CreatedAt DATE
	)
	PARTITIONED BY (CreatedAt_P DATE)
	STORED AS ORC;

-- HIVE: DW_WISHHACK.DIM_PRODUCT
CREATE TABLE DW_WISHHACK.DIM_PRODUCT(
  ProductID STRING
  , ManufacturerID STRING
  , MaxDeliveryTime TIMESTAMP
  , MaxFulfillmentTime TIMESTAMP
  , MaxShipTime TIMESTAMP
  , MinDeliveryTime TIMESTAMP
  , MinFulfillmentTime TIMESTAMP
  , MinShipTime TIMESTAMP
  , Price DECIMAL(10,2)
  , IsC2c STRING
  , IsFbw STRING
  , IsFreeGift STRING
  , IsFreeSample STRING
  , IsHiddenMysteryBoxItem STRING
  , IsMysteryBox STRING
  , IsWishExpress STRING
  , CreatedAt DATE
    )
    PARTITIONED BY (CreatedAt_P DATE)
    STORED AS ORC;

-- HIVE: DW_WISHHACK.DIM_MERCHANT
CREATE TABLE DW_WISHHACK.DIM_MERCHANT(
  MerchantID STRING
  , MerchantFullName STRING
  , MerchantShortName STRING
  , MerchantDisplayName STRING
  , IsStrategic STRING
  , IsWishOwned STRING
  , CreatedAt DATE
    )
    PARTITIONED BY (CreatedAt_P DATE)
    STORED AS ORC;

-- HIVE: DW_WISHHACK.FACT_SALES
CREATE TABLE DW_WISHHACK.FACT_SALES(
  CustomerID STRING
  , ProductID STRING
  , MerchantID STRING
  , TransactionID STRING
  , TransactionDate STRING
  , IsGift STRING
  , IsNewRefund STRING
  , PaymentID STRING
  , PaymentName STRING
  , PaymentType STRING
  , PaymentTypeNumber STRING
  , Subtotal DECIMAL(10,2)
  , Taxes DECIMAL(10,2)
  , Total DECIMAL(10,2)
  , CreatedAt STRING
    )
    PARTITIONED BY (CreatedAt_P STRING)
    STORED AS ORC;

-- HIVE: DW_WISHHACK.WISHLIST_ATTR
CREATE TABLE DW_WISHHACK.WISHLIST_ATTR(
  UserID STRING
  , ListID STRING
  , ListName STRING
  , Private STRING
  , ItemID STRING
  , CreatedAt DATE
    )
    PARTITIONED BY (CreatedAt_P DATE)
    STORED AS ORC;

-- HIVE: DW_WISHHACK.WISHLIST_DETAIL_ATTR
CREATE TABLE DW_WISHHACK.WISHLIST_DETAIL_ATTR(
  ItemID STRING
  , ItemName STRING
  , SellerPrice DECIMAL(10,2)
  , CatID STRING
  , ParentCatID STRING
  , IsParent STRING
  , CategoryName STRING
  , CategoryLevel STRING
  , CreatedAt DATE
    )
    PARTITIONED BY (CreatedAt_P DATE)
    STORED AS ORC;

-- HIVE: DW_WISHHACK.PRODUCT_REVIEW_ATTR
CREATE TABLE DW_WISHHACK.PRODUCT_REVIEW_ATTR(
  ReviewID STRING
  , ProductID STRING
  , Rating STRING
  , DateReview TIMESTAMP
  , UpvoteCount STRING
  , UserID STRING
  , TransactionID STRING
  , CreatedAt DATE
    )
    PARTITIONED BY (CreatedAt_P DATE)
    STORED AS ORC;

-- HIVE: DW_WISHHACK.PRODUCT_TAG_ATTR
CREATE TABLE DW_WISHHACK.PRODUCT_TAG_ATTR(
  ProductID STRING
  , ProductTag STRING
  , CreatedAt DATE
    )
    PARTITIONED BY (CreatedAt_P DATE)
    STORED AS ORC;

-- HIVE: DW_WISHHACK.FACT_SALES
CREATE TABLE DW_WISHHACK.FACT_SALES_AGG_YEAR(
  CustomerID STRING
  , ProductID STRING
  , MerchantID STRING
  , TransactionYear STRING
  , IsGift STRING
  , IsNewRefund STRING
  , PaymentName STRING
  , PaymentType STRING
  , Subtotal DECIMAL(10,2)
  , Taxes DECIMAL(10,2)
  , Total DECIMAL(10,2)
  , TransactionDate DATE
    )
    PARTITIONED BY (TransactionDate_P DATE)
    STORED AS ORC;

-- HIVE: DW_WISHHACK.FACT_SALES
CREATE TABLE DW_WISHHACK.FACT_SALES_AGG_MONTH(
  CustomerID STRING
  , ProductID STRING
  , MerchantID STRING
  , TransactionYear STRING
  , TransactionMonth STRING
  , IsGift STRING
  , IsNewRefund STRING
  , PaymentName STRING
  , PaymentType STRING
  , Subtotal DECIMAL(10,2)
  , Taxes DECIMAL(10,2)
  , Total DECIMAL(10,2)
  , TransactionDate DATE
    )
    PARTITIONED BY (TransactionDate_P DATE)
    STORED AS ORC;

-- HIVE: DW_WISHHACK.FACT_SALES
CREATE TABLE DW_WISHHACK.FACT_SALES_AGG_DAY(
  CustomerID STRING
  , ProductID STRING
  , MerchantID STRING
  , TransactionYear STRING
  , TransactionMonth STRING
  , TransactionDay STRING
  , IsGift STRING
  , IsNewRefund STRING
  , PaymentName STRING
  , PaymentType STRING
  , Subtotal DECIMAL(10,2)
  , Taxes DECIMAL(10,2)
  , Total DECIMAL(10,2)
  , TransactionDate DATE
    )
    PARTITIONED BY (TransactionDate_P DATE)
    STORED AS ORC;
