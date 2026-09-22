select ca_zip
      ,sum(cs_sales_price) as total_sales_price
      ,any_value(ca_city) as any_city
      ,any_value(ca_state) as any_state
      ,count(distinct c_email_address) as distinct_email_count
      ,any_value(c_birth_country) as any_birth_country
      ,min(d_date) as min_sold_date
      ,max(cast(d_date as timestamp)) as max_sold_ts
 from catalog_sales
     ,customer
     ,customer_address
     ,date_dim
 where cs_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk 
 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
 	      or ca_state in ('CA','WA','GA')
	      or ca_country = 'United States')
 	and cs_sold_date_sk = d_date_sk
 	and d_qoy = 2 and d_year = 2002
        and c_preferred_cust_flag = 'Y'
        and c_birth_country in ('UNITED STATES','CANADA','MEXICO')
 group by ca_zip
 order by total_sales_price desc
         ,max_sold_ts desc
         ,min_sold_date desc

;
