select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag 
       ,ss_ticket_number
       ,cnt
       ,max_store_name
       ,max_store_state
       ,any_buy_potential
       ,min_sold_date
       ,max_sold_ts from
   (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
          ,max(store.s_store_name) as max_store_name
          ,max(store.s_state) as max_store_state
          ,any_value(household_demographics.hd_buy_potential) as any_buy_potential
          ,min(date_dim.d_date) as min_sold_date
          ,max(cast(date_dim.d_date as timestamp)) as max_sold_ts
    from store_sales,date_dim,store,household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and date_dim.d_dom between 1 and 2 
    and (household_demographics.hd_buy_potential = '501-1000' or
         household_demographics.hd_buy_potential = '5001-10000')
    and household_demographics.hd_vehicle_count > 0
    and case when household_demographics.hd_vehicle_count > 0 then 
             household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count else null end > 1
    and date_dim.d_year in (2000,2000+1,2000+2)
    and store.s_county in ('Williamson County','Walker County','Ziebach County','Walker County')
    group by ss_ticket_number,ss_customer_sk) dj,customer
    where ss_customer_sk = c_customer_sk
      and cnt between 1 and 5
      and c_preferred_cust_flag = 'Y'
      and c_birth_country is not null
    order by cnt desc, max_sold_ts desc, min_sold_date desc

;
