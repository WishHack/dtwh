# =============================================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# About this Code:  script to move data from stage area to data warehouse into Hive
# =============================================================================================
# Version 1: creating the code
# =============================================================================================

%sql
-- Loading data to FACT_SALES
SET HIVE.EXEC.DYNAMIC.PARTITION.MODE=NOSTRICT;

-- APPEND MODE
INSERT INTO TABLE DW_WISHHACK.FACT_SALES PARTITION(CreatedAt_P)
SELECT
    OD.USER_ID AS CustomerID
    , 'NA' AS ProductID
    , OD.MERCHANT_ID AS MerchantID
    , OD.TRANSACTION_ID AS TransactionID
    , OD.TIME AS TransactionDate
    , OD.TRANSACTION_IS_GIFT AS IsGift
    , OD.TRANSACTION_IS_NEW_REFUND AS IsNewRefund
    , OD.PAYMENT_ID AS PaymentID
    , OD.PAYMENT_NAME AS PaymentName
    , OD.PAYMENT_TYPE AS PaymentType
    , OD.PAYMENT_TYPE_NUMBER AS PaymentTypeNumber
    , OD.SUBTOTAL AS Subtotal
    , 0 AS Taxes
    , OD.TOTAL AS Total
    , SUBSTRING(CURRENT_DATE,1,10) AS CreatedAt
    , SUBSTRING(CURRENT_DATE,1,10) AS CreatedAt_P
FROM ORDERDETAILS_TEST_STG OD

-- OVERWRITE
INSERT OVERWRITE TABLE DW_WISHHACK.FACT_SALES PARTITION(CreatedAt_P)
SELECT
    OD.USER_ID AS CustomerID
    , 'NA' AS ProductID
    , OD.MERCHANT_ID AS MerchantID
    , OD.TRANSACTION_ID AS TransactionID
    , OD.TIME AS TransactionDate
    , OD.TRANSACTION_IS_GIFT AS IsGift
    , OD.TRANSACTION_IS_NEW_REFUND AS IsNewRefund
    , OD.PAYMENT_ID AS PaymentID
    , OD.PAYMENT_NAME AS PaymentName
    , OD.PAYMENT_TYPE AS PaymentType
    , OD.PAYMENT_TYPE_NUMBER AS PaymentTypeNumber
    , OD.SUBTOTAL AS Subtotal
    , 0 AS Taxes
    , OD.TOTAL AS Total
    , SUBSTRING(CURRENT_DATE,1,10) AS CreatedAt
    , SUBSTRING(CURRENT_DATE,1,10) AS CreatedAt_P
FROM ORDERDETAILS_TEST_STG OD
