select  c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,extended_price
       ,dn.any_buy_potential as any_buy_potential
       ,dn.any_store_name as any_store_name
       ,dn.any_market_desc as any_market_desc
       ,count(distinct current_addr.ca_state) as distinct_current_state_count
       ,dn.min_sold_date as min_sold_date
       ,dn.max_sold_ts as max_sold_ts
 from (select ss_ticket_number
             ,ss_customer_sk
             ,ca_city bought_city
             ,sum(ss_ext_sales_price) extended_price
             ,sum(ss_ext_list_price) list_price
             ,sum(ss_ext_tax) extended_tax
             ,any_value(household_demographics.hd_buy_potential) as any_buy_potential
             ,any_value(store.s_store_name) as any_store_name
             ,any_value(store.s_market_desc) as any_market_desc
             ,min(d_date) as min_sold_date
             ,max(cast(d_date as timestamp)) as max_sold_ts
       from store_sales
           ,date_dim
           ,store
           ,household_demographics
           ,customer_address
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
        and store_sales.ss_store_sk = store.s_store_sk
       and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        and store_sales.ss_addr_sk = customer_address.ca_address_sk
        and date_dim.d_dom between 1 and 2
        and household_demographics.hd_buy_potential in ('1001-5000','501-1000','>10000')
        and date_dim.d_year in (1998,1998+1,1998+2)
        and store.s_market_desc is not null
        and store.s_city in ('Pleasant Hill','Oak Grove')
       group by ss_ticket_number
               ,ss_customer_sk
               ,ss_addr_sk,ca_city) dn
      ,customer
      ,customer_address current_addr
 where ss_customer_sk = c_customer_sk
   and customer.c_current_addr_sk = current_addr.ca_address_sk
   and current_addr.ca_city <> bought_city
   and current_addr.ca_country = 'United States'
   and current_addr.ca_state in ('CA','WA','GA','TX')
   and current_addr.ca_city is not null
   and c_birth_country in ('AMERICAN SAMOA','MOZAMBIQUE')
 group by c_last_name
         ,c_first_name
         ,ca_city
         ,bought_city
         ,ss_ticket_number
         ,extended_price
         ,dn.any_buy_potential
         ,dn.any_store_name
         ,dn.any_market_desc
         ,dn.min_sold_date
         ,dn.max_sold_ts
 order by extended_price desc
         ,max_sold_ts desc
         ,min_sold_date desc
  limit 10000

;
