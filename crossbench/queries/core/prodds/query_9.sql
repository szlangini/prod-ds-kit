select i_item_id
,i_item_desc
,s_store_id
,s_store_name
,avg(ss_net_profit) as store_sales_profit
,any_value(i_category) as any_item_category
,any_value(i_brand) as any_item_brand
,any_value(s_market_desc) as any_market_desc
,max(i_product_name) as max_product_name
,count(distinct s_city) as distinct_city_count
,any_value(s_company_name) as any_company_name
,max(s_division_name) as max_division_name
,min(d1.d_date) as min_ss_date
,max(d3.d_date) as max_cs_date
,max(cast(d2.d_date as timestamp)) as max_return_ts
 from
 store_sales
 ,store_returns
 ,catalog_sales
 ,date_dim d1
 ,date_dim d2
 ,date_dim d3
 ,store
 ,item
 where
 d1.d_moy = 3
 and d1.d_year = 1998
 and d1.d_date_sk = ss_sold_date_sk
 and i_item_sk = ss_item_sk
 and s_store_sk = ss_store_sk
 and ss_customer_sk = sr_customer_sk
 and ss_item_sk = sr_item_sk
 and ss_ticket_number = sr_ticket_number
 and sr_returned_date_sk = d2.d_date_sk
 and d2.d_moy               between 3 and  9
 and d2.d_year              = 1998
 and sr_customer_sk = cs_bill_customer_sk
 and sr_item_sk = cs_item_sk
 and cs_sold_date_sk = d3.d_date_sk
 and d3.d_moy               between 3 and  9 
 and d3.d_year              = 1998
 group by
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name
 order by
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name

;
