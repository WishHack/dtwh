# -*- coding: utf-8 -*-

# =============================================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# About this Code:  extract all data from table "order_details" based on filter "merchant_id"
#                   and save the results in CSV file.
# DB Source: Hive -> order_details
# Target: CSV file
# =============================================================================================
# Version 1: creating the code
# Version 2: query changed
# =============================================================================================

import csv
from pyhive import hive


def cnn_dw():
    """
    Before is necessary to install these libraries:
    pip install sasl
    pip install thrift
    pip install thrift-sasl
    pip install pyhive
    """

    conn = hive.Connection(host="47.74.228.141",
                           port=10000,
                           username="root")

    cursor = conn.cursor()

    strSQL = """
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
    """

    cursor.execute(strSQL)

    rows = 0

    wishhack = open('/home/lserra/PycharmProjects/wishhack/merchant.csv', 'w')

    with wishhack:
        strFields = ['merchant_id', 'open_id', 'transaction_id', 'transaction_ip_address',
                     'transaction_email', 'transaction_login_method', 'transaction_verified',
                     'mtid', 'shipping_city', 'shipping_country', 'shipping_country_code',
                     'shipping_name', 'shipping_phone_number', 'shipping_state', 'shipping_state_abbreviation',
                     'shipping_address1', 'shipping_address2', 'shipping_zipcode', 'transaction_time',
                     'transaction_subtotal', 'transaction_total', 'user_id', 'product_id', 'image_url',
                     'is_wish_express', 'merchant_display_name', 'merchant_name', 'merchant_price', 'product_name',
                     'original_price', 'original_shipping', 'quantity', 'shipped_date', 'size', 'variation_id',
                     'is_my_product']

        writer = csv.DictWriter(wishhack, fieldnames=strFields)
        writer.writeheader()

        for each_row in cursor.fetchall():
            writer.writerow({'merchant_id': each_row[0],
                             'open_id': each_row[1],
                             'transaction_id': each_row[2],
                             'transaction_ip_address': each_row[3],
                             'transaction_email': each_row[4],
                             'transaction_login_method': each_row[5],
                             'transaction_verified': each_row[6],
                             'mtid': each_row[7],
                             'shipping_city': each_row[8],
                             'shipping_country': each_row[9],
                             'shipping_country_code': each_row[10],
                             'shipping_name': each_row[11],
                             'shipping_phone_number': each_row[12],
                             'shipping_state': each_row[13],
                             'shipping_state_abbreviation': each_row[14],
                             'shipping_address1': each_row[15],
                             'shipping_address2': each_row[16],
                             'shipping_zipcode': each_row[17],
                             'transaction_time': each_row[18],
                             'transaction_subtotal': each_row[19],
                             'transaction_total': each_row[20],
                             'user_id': each_row[21],
                             'product_id': each_row[22],
                             'image_url': each_row[23],
                             'is_wish_express': each_row[24],
                             'merchant_display_name': each_row[25],
                             'merchant_name': each_row[26],
                             'merchant_price': each_row[27],
                             'product_name': each_row[28],
                             'original_price': each_row[29],
                             'original_shipping': each_row[30],
                             'quantity': each_row[31],
                             'shipped_date': each_row[32],
                             'size': each_row[33],
                             'variation_id': each_row[34],
                             'is_my_product': each_row[35]
                             })
            rows += 1
            print(each_row)

    print("-" * 150)
    print(">>> Total rows =", rows)


if __name__ == '__main__':
    cnn_dw()
