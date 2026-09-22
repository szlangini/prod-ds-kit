select s_store_name, s_store_id,
        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales,
        any_value(s_city) as any_store_city,
        any_value(s_state) as any_store_state,
        any_value(s_market_desc) as any_market_desc
 from date_dim, store_sales, store
 where d_date_sk = ss_sold_date_sk and
       s_store_sk = ss_store_sk and
       s_gmt_offset = -6 and
       d_year = 1998 and
       s_state in ('AL','TN','SD')
       and s_market_desc is not null
       and s_city in ('Five Points','Midway','Riverside')
       and s_company_name is not null
 group by s_store_name, s_store_id
 order by sun_sales desc,mon_sales desc,tue_sales desc,wed_sales desc,thu_sales desc,fri_sales desc,sat_sales desc

;
