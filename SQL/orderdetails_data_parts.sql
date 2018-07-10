==========================================================

OK-[ DC ] user_id
OK-[ DC ] transaction_ip_address
OK-[ DC ] transaction_email
OK-[ DC ] transaction_login_method
OK-[ DC ] transaction_verified
OK-[ DC ] shipping_name
OK-[ DC ] shipping_phone_number
OK-[ DC ] shipping_address1
OK-[ DC ] shipping_address2
OK-[ DC ] shipping_city
OK-[ DC ] shipping_state
OK-[ DC ] shipping_state_abbreviation
OK-[ DC ] shipping_country
OK-[ DC ] shipping_country_code
OK-[ DC ] shipping_zipcode

==========================================================

OK-[ DT ] transaction_time

==========================================================

OK-[ DP ] product_id
OK-[ DP ] product_name
OK-[ DP ] size
OK-[ DP ] is_wish_express
OK-[ DP ] original_price
OK-[ DP ] original_shipping
OK-[ DP ] image_url

==========================================================

OK-[ DS ] merchant_id
OK-[ DS ] merchant_display_name
OK-[ DS ] merchant_name
OK-[ DS ] merchant_price

==========================================================

OK-[ FS ] transaction_id
OK-[ FS ] user_id
OK-[ FS ] time_id
OK-[ FS ] product_id
OK-[ FS ] merchant_id
OK-[ FS ] variation_id
OK-[ FS ] mtid
OK-[ FS ] transaction_subtotal
OK-[ FS ] transaction_total
OK-[ FS ] quantity
OK-[ DT ] transaction_time
OK-[ FS ] shipped_date

==========================================================

FS => Fact_Sales
DC => Dim_Customer
DP => Dim_Product
DS => Dim_Store
DT => Dim_Time