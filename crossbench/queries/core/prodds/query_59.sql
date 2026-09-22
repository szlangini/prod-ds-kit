select dt.d_year
	,item.i_brand_id brand_id
	,item.i_brand brand
	,sum(ss_ext_sales_price) ext_price
        ,any_value(item.i_category) as any_category_label
        ,count(distinct item.i_product_name) as distinct_product_name_count
        ,max(item.i_product_name) as max_product_name
        ,any_value(item.i_manufact) as any_manufact
        ,count(distinct item.i_class) as distinct_class_count
        ,min(dt.d_date) as min_sold_date
        ,max(cast(dt.d_date as timestamp)) as max_sold_ts
 from date_dim dt
     ,store_sales
     ,item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
    and store_sales.ss_item_sk = item.i_item_sk
    and dt.d_moy=12
    and dt.d_year=2002
    and item.i_category in ('Home','Electronics','Sports')
    and item.i_brand is not null
 group by dt.d_year
 	,item.i_brand
 	,item.i_brand_id
 order by dt.d_year
 	,ext_price desc
 	,brand_id

;
