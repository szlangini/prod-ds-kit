select i_item_id
      ,i_item_desc
      ,s_state
      ,count(ss_quantity) as store_sales_quantitycount
      ,avg(sr_return_quantity) as store_returns_quantityave
      ,count(cs_quantity) as catalog_sales_quantitycount
      ,any_value(i_category) as any_item_category
      ,count(distinct s_store_name) as distinct_store_name_count
      ,any_value(s_market_desc) as any_market_desc
      ,any_value(i_brand) as any_item_brand
      ,max(i_product_name) as max_product_name
      ,count(distinct i_class) as distinct_item_class_count
      ,max(s_division_name) as max_division_name
      ,min(d1.d_date) as min_ss_sold_date
      ,max(d2.d_date) as max_return_date
      ,max(cast(d3.d_date as timestamp)) as max_cs_sold_ts
 from store_sales
    ,store_returns
    ,catalog_sales
     ,date_dim d1
     ,date_dim d2
     ,date_dim d3
     ,store
     ,item
 where d1.d_quarter_name = '2000Q1'
   and d1.d_date_sk = ss_sold_date_sk
   and i_item_sk = ss_item_sk
   and s_store_sk = ss_store_sk
   and i_category in ('Books','Electronics','Sports')
   and ss_customer_sk = sr_customer_sk
   and ss_item_sk = sr_item_sk
   and ss_ticket_number = sr_ticket_number
   and sr_returned_date_sk = d2.d_date_sk
   and d2.d_quarter_name in ('2000Q1','2000Q2','2000Q3')
   and sr_customer_sk = cs_bill_customer_sk
   and sr_item_sk = cs_item_sk
   and cs_sold_date_sk = d3.d_date_sk
   and d3.d_quarter_name in ('2000Q1','2000Q2','2000Q3')
 group by i_item_id
         ,i_item_desc
         ,s_state
         ,i_category
         ,i_brand
         ,i_class
         ,i_manufact_id
         ,i_manufact
         ,s_store_name
         ,s_market_desc
         ,s_division_name
 order by store_sales_quantitycount desc
         ,max_cs_sold_ts desc
         ,min_ss_sold_date desc

;
