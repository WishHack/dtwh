# =============================================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# About this Code: script to move data from stage area to data warehouse into Hive
# =============================================================================================
# Version 1: creating the code
# =============================================================================================

%sql
-- Loading data to DIM_PERIOD
CREATE TABLE IF NOT EXISTS DW_WISHHACK.DIM_PERIOD AS
SELECT
    CONCAT(YEAR(D), '0', MONTH(D), '0', DAY(D)) AS  time_id
,   D                                           AS  short_date
,   YEAR(D)                                     AS  year
,   QUARTER(D)                                  AS  quarter
,   MONTH(D)                                    AS  month
,   DAY(D)                                      AS  day
,   WEEKOFYEAR(D)                               AS  week_of_year
,   CASE
        WHEN
            MONTH(D)    =   1
        THEN
            'january'
        WHEN
            MONTH(D)    =   2
        THEN
            'february'
        WHEN
            MONTH(D)    =   3
        THEN
            'march'
        WHEN
            MONTH(D)    =   4
        THEN
            'april'
        WHEN
            MONTH(D)    =   5
        THEN
            'may'
        WHEN
            MONTH(D)    =   6
        THEN
            'june'
        WHEN
            MONTH(D)    =   7
        THEN
            'july'
        WHEN
            MONTH(D)    =   8
        THEN
            'august'
        WHEN
            MONTH(D)    =   9
        THEN
            'september'
        WHEN
            MONTH(D)    =   10
        THEN
            'october'
        WHEN
            MONTH(D)    =   11
        THEN
            'november'
        WHEN
            MONTH(D)    =   12
        THEN
            'december'
        ELSE
            'NA'
    END month_name
FROM
    (
        SELECT
            DATE_ADD("2015-01-01", A.POS)   AS  D
        FROM
            (
                SELECT
                    POSEXPLODE(SPLIT(REPEAT("O", DATEDIFF("2017-12-31", "2015-01-01")), "O"))
            )   A
    )   DATES   
SORT BY
    D