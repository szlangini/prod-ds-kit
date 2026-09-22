select dt.d_year 
       ,item.i_brand_id brand_id 
       ,item.i_brand brand
       ,sum(ss_sales_price) sum_agg
       ,any_value(item.i_category) as any_category_label
       ,count(distinct item.i_product_name) as distinct_product_name_count
 from  date_dim dt 
      ,store_sales
      ,item
where dt.d_date_sk = store_sales.ss_sold_date_sk
   and store_sales.ss_item_sk = item.i_item_sk
   and dt.d_moy=11
   and item.i_category in ('Home','Electronics','Sports')
   and dt.d_day_name in ('Monday','Tuesday','Wednesday')
   and item.i_color is not null
 group by dt.d_year
      ,item.i_brand
      ,item.i_brand_id
 order by dt.d_year
         ,sum_agg desc
         ,brand_id

;
