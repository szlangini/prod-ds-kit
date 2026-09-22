select 
 c_last_name,c_first_name,substr(s_city,1,30),ss_ticket_number,profit
 ,max_store_name
 ,any_market_desc
 ,any_buy_potential
 ,min_sold_date
 ,max_sold_ts
  from
   (select ss_ticket_number
          ,ss_customer_sk
          ,store.s_city
          ,sum(ss_net_profit) profit
          ,max(store.s_store_name) as max_store_name
          ,any_value(store.s_market_desc) as any_market_desc
          ,any_value(household_demographics.hd_buy_potential) as any_buy_potential
          ,min(date_dim.d_date) as min_sold_date
          ,max(cast(date_dim.d_date as timestamp)) as max_sold_ts
    from store_sales,date_dim,store,household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and household_demographics.hd_buy_potential in ('1001-5000','501-1000','>10000')
    and date_dim.d_dow = 1
    and date_dim.d_year in (1998,1998+1,1998+2) 
    and store.s_city in ('Five Points','Midway','Riverside')
    and store.s_market_desc is not null
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,store.s_city) ms,customer
    where ss_customer_sk = c_customer_sk
      and c_preferred_cust_flag = 'Y'
 order by profit desc,max_sold_ts desc,min_sold_date desc
 limit 10000

;
