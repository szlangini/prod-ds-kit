select i_item_id
       ,i_item_desc 
       ,i_category 
       ,i_class 
       ,count(*) as item_count
       ,any_value(i_brand) as any_brand_label
       ,any_value(i_manufact) as any_manufact_label
       ,min(d_date) as min_sold_date
       ,max(cast(d_date as timestamp)) as max_sold_ts
 from	catalog_sales
    ,item 
    ,date_dim
where cs_item_sk = i_item_sk 
  and i_category in ('Women', 'Shoes', 'Sports')
  and i_brand is not null
  and i_product_name is not null
  and i_brand like 'univ%'
  and i_manufact is not null
  and cs_sold_date_sk = d_date_sk
 and d_date between cast('2000-06-30' as date) 
				and (cast('2000-06-30' as date) + interval '30 days')
 group by i_item_id
         ,i_item_desc 
         ,i_category
         ,i_class
         ,i_current_price
 order by max_sold_ts desc
         ,item_count desc
         ,min_sold_date

;
