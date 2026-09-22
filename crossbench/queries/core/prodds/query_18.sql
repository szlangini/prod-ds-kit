select ca_zip, ca_city, sum(ws_sales_price) as total_sales_price
        ,any_value(ca_country) as any_country
        ,count(distinct c_email_address) as distinct_email_count
        ,any_value(c_birth_country) as any_birth_country
        ,min(d_date) as min_sold_date
        ,max(cast(d_date as timestamp)) as max_sold_ts
 from web_sales, customer, customer_address, date_dim, item
 where ws_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk 
 	and ws_item_sk = i_item_sk 
 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')
 	      or 
              i_category in ('Home','Electronics','Sports')
              )
 	and ws_sold_date_sk = d_date_sk
 	and d_qoy = 2 and d_year = 1998
        and ca_country = 'United States'
        and ca_state in ('CA','TX','NY','WA')
        and c_preferred_cust_flag = 'Y'
 group by ca_zip, ca_city
 order by total_sales_price desc, max_sold_ts desc, min_sold_date desc

;
