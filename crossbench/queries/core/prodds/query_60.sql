select s_store_name
  ,s_company_id
  ,s_street_number
  ,s_street_name
  ,s_street_type
  ,s_suite_number
  ,s_city
  ,s_county
  ,s_state
  ,s_zip
  ,s_country
  ,s_manager
  ,sum(case when ((cast(regexp_replace(cast(sr_returned_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(ss_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) > cast(regexp_replace(cast(0 as text), '[^0-9-]', '', 'g') as bigint))) then 1 else 0 end)  as ">120 days" 
  ,any_value(s_market_desc) as any_market_desc
  ,any_value(s_company_name) as any_company_name
  ,any_value(s_manager) as any_store_manager
  ,max(cast(d2.d_date as timestamp)) as max_return_ts
  ,min(d2.d_date) as min_return_date
from
   store_sales
  ,store_returns
  ,store
  ,date_dim d1
  ,date_dim d2
where
    d2.d_year = 1999
and d2.d_moy  = 8
and ss_ticket_number = sr_ticket_number
and ss_item_sk = sr_item_sk
and ss_sold_date_sk   = d1.d_date_sk
and sr_returned_date_sk   = d2.d_date_sk
and ss_customer_sk = sr_customer_sk
and ss_store_sk = s_store_sk
and s_company_name is not null
and s_state in ('SD','TN','AL')
and s_city is not null
and s_manager is not null
and s_market_desc is not null
group by
   s_store_name
  ,s_company_id
  ,s_street_number
  ,s_street_name
  ,s_street_type
  ,s_suite_number
  ,s_city
  ,s_county
  ,s_state
  ,s_zip
  ,s_country
  ,s_manager
 order by max_return_ts desc
        ,min_return_date
        ,s_store_name

;
