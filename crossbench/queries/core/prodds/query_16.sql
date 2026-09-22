select i_item_id,
       s_state, grouping(s_state) g_state,
       avg(ss_sales_price) agg4,
       any_value(i_item_desc) as any_item_desc,
       any_value(i_category) as any_item_category,
       max(i_brand) as max_item_brand,
       count(distinct i_product_name) as distinct_product_name_count,
       max(s_store_name) as max_store_name,
       count(distinct s_city) as distinct_city_count,
       min(d_date) as min_sold_date,
       max(cast(d_date as timestamp)) as max_sold_ts
 from store_sales, customer_demographics, date_dim, store, item
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_store_sk = s_store_sk and
       ss_cdemo_sk = cd_demo_sk and
       cd_gender = 'M' and
       cd_marital_status = 'U' and
       cd_education_status = 'Unknown' and
       i_category in ('Books','Electronics','Music') and
       d_year = 1998 and
       s_state in ('AL','TN', 'SD', 'SD', 'SD', 'SD')
 group by rollup (i_item_id, s_state)
 order by agg4 desc
         ,max_sold_ts desc
         ,min_sold_date desc

;
