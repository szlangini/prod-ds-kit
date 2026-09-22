select c_last_name
      ,c_first_name
      ,ca_city
      ,bought_city
      ,ss_ticket_number
      ,profit
      ,max_store_name
      ,any_company_name
      ,max_bought_state
      ,distinct_store_city_count
      ,any_buy_potential
      ,min_sold_date
      ,max_sold_ts 
 from
  (select ss_ticket_number
         ,ss_customer_sk
         ,ca_city bought_city
         ,sum(ss_net_profit) profit
         ,max(store.s_store_name) as max_store_name
         ,any_value(store.s_company_name) as any_company_name
         ,max(customer_address.ca_state) as max_bought_state
         ,count(distinct store.s_city) as distinct_store_city_count
         ,any_value(household_demographics.hd_buy_potential) as any_buy_potential
         ,min(date_dim.d_date) as min_sold_date
         ,max(cast(date_dim.d_date as timestamp)) as max_sold_ts
    from store_sales,date_dim,store,household_demographics,customer_address 
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and store_sales.ss_addr_sk = customer_address.ca_address_sk
    and household_demographics.hd_buy_potential in ('1001-5000','501-1000','>10000')
    and date_dim.d_dow in (6,0)
    and date_dim.d_year in (1999,1999+1,1999+2) 
    and store.s_city in ('Midway','Five Points','Pleasant Hill','Oak Grove','Fairview') 
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city) dn,customer,customer_address current_addr
    where ss_customer_sk = c_customer_sk
      and customer.c_current_addr_sk = current_addr.ca_address_sk
      and current_addr.ca_city <> bought_city
      and c_birth_country in ('MEXICO','DENMARK')
      and current_addr.ca_state in ('CA','TX','NY','WA')
  order by c_last_name
          ,c_first_name
          ,ca_city
          ,bought_city
          ,ss_ticket_number

;
