select c_last_name
      ,c_first_name
      ,c_salutation
      ,c_preferred_cust_flag
      ,ss_ticket_number
      ,cnt
      ,max_store_name
      ,max_store_state
      ,any_buy_potential
      ,any_company_name
      ,min_sold_date
      ,max_sold_ts from
  (select ss_ticket_number
         ,ss_customer_sk
         ,count(*) cnt
         ,max(store.s_store_name) as max_store_name
         ,max(store.s_state) as max_store_state
         ,any_value(household_demographics.hd_buy_potential) as any_buy_potential
         ,any_value(store.s_company_name) as any_company_name
         ,min(date_dim.d_date) as min_sold_date
         ,max(cast(date_dim.d_date as timestamp)) as max_sold_ts
    from store_sales,date_dim,store,household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and (date_dim.d_dom between 1 and 3 or date_dim.d_dom between 25 and 28)
    and (household_demographics.hd_buy_potential = '>10000' or
         household_demographics.hd_buy_potential = '5001-10000')
    and household_demographics.hd_vehicle_count > 0
    and (case when household_demographics.hd_vehicle_count > 0 
	then household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count 
	else null 
	end)  > 1.2
    and date_dim.d_year in (1999,1999+1,1999+2)
    and store.s_county in ('Walker County','Ziebach County','Williamson County','Ziebach County',
                           'Ziebach County','Ziebach County','Ziebach County','Ziebach County')
    group by ss_ticket_number,ss_customer_sk) dn,customer
    where ss_customer_sk = c_customer_sk
      and cnt between 15 and 20
      and c_preferred_cust_flag = 'Y'
      and c_birth_country is not null
    order by cnt desc,max_sold_ts desc,min_sold_date desc

;
