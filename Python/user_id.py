# -*- coding: utf-8 -*-

# =============================================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# About this Code:	extract all data from table "order_details" based on filter "user_id"
# 					and save the results in CSV file.
# DB Source: Hive -> order_details
# =============================================================================================
# Version 1: creating the code
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
    select
        distinct user_id
    from
        wishhack.orderdetails_full_23062018_stg
    """

    cursor.execute(strSQL)

    rows = 0

    wishhack = open('/home/lserra/PycharmProjects/wishhack/user.csv', 'w')

    with wishhack:
        strFields = ['user_id']

        writer = csv.DictWriter(wishhack, fieldnames=strFields)
        writer.writeheader()

        for each_row in cursor.fetchall():
            writer.writerow({
                             'user_id': each_row[0]
                             })
            rows += 1
            print(each_row)

    print("-" * 150)
    print(">>> Total rows =", rows)


if __name__ == '__main__':
    cnn_dw()
