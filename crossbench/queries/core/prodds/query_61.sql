select dt.d_year
	,item.i_category_id
	,item.i_category
	,sum(ss_ext_sales_price) as total_sales
        ,any_value(item.i_brand) as any_brand
        ,max(item.i_product_name) as max_product_name
        ,count(distinct item.i_class) as distinct_class_count
        ,min(dt.d_date) as min_sold_date
        ,max(cast(dt.d_date as timestamp)) as max_sold_ts
 from 	date_dim dt
 	,store_sales
 	,item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
 	and store_sales.ss_item_sk = item.i_item_sk
 	and dt.d_moy=11
 	and dt.d_year=2000
        and item.i_brand is not null
        and item.i_class is not null
 group by 	dt.d_year
 		,item.i_category_id
 		,item.i_category
 order by       sum(ss_ext_sales_price) desc,dt.d_year
 		,item.i_category_id
 		,item.i_category

;
