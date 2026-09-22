select i_item_id, 
       avg(ss_sales_price) agg4,
       any_value(i_item_desc) as any_item_desc,
       any_value(i_category) as any_item_category,
       max(i_brand) as max_item_brand,
       count(distinct i_product_name) as distinct_product_name_count,
       any_value(p_channel_event) as any_channel_event,
       min(d_date) as min_sold_date,
       max(cast(d_date as timestamp)) as max_sold_ts
 from store_sales, customer_demographics, date_dim, item, promotion
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_cdemo_sk = cd_demo_sk and
       ss_promo_sk = p_promo_sk and
       cd_gender = 'M' and 
       cd_marital_status = 'M' and
       cd_education_status = '4 yr Degree' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 2001 
 group by i_item_id
 order by i_item_id

;
