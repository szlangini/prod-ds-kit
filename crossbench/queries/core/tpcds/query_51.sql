with year_total as materialized (
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum(ss_ext_list_price-ss_ext_discount_amt) year_total
       ,'s' sale_type
 from customer
     ,store_sales
     ,date_dim
 where c_customer_sk = ss_customer_sk
   and ss_sold_date_sk = d_date_sk
   and d_year in (1998,1999)
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag 
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year 
 union all
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum(ws_ext_list_price-ws_ext_discount_amt) year_total
       ,'w' sale_type
 from customer
     ,web_sales
     ,date_dim
 where c_customer_sk = ws_bill_customer_sk
   and ws_sold_date_sk = d_date_sk
   and d_year in (1998,1999)
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag 
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year
         )
 select customer_id,
       customer_first_name,
       customer_last_name,
       customer_preferred_cust_flag
 from year_total
 group by customer_id,
       customer_first_name,
       customer_last_name,
       customer_preferred_cust_flag
 having max(case when sale_type = 's' and dyear = 1998 then year_total end) > 0
   and max(case when sale_type = 'w' and dyear = 1998 then year_total end) > 0
   and (max(case when sale_type = 'w' and dyear = 1999 then year_total end) / nullif(max(case when sale_type = 'w' and dyear = 1998 then year_total end), 0)) > (max(case when sale_type = 's' and dyear = 1999 then year_total end) / nullif(max(case when sale_type = 's' and dyear = 1998 then year_total end), 0))
 order by customer_id,
       customer_first_name,
       customer_last_name,
       customer_preferred_cust_flag
 limit 100

;
