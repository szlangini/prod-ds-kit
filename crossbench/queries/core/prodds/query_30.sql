with wss as 
 (select d_week_seq,
        ss_store_sk,
        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales,
        min(d_date) as min_week_date,
        max(d_date) as max_week_date
 from store_sales,date_dim
 where d_date_sk = ss_sold_date_sk
 group by d_week_seq,ss_store_sk
 ) select s_store_name1,s_store_id1,d_week_seq1
       ,sun_sales1/sun_sales2,mon_sales1/mon_sales2
       ,tue_sales1/tue_sales2,wed_sales1/wed_sales2,thu_sales1/thu_sales2
       ,fri_sales1/fri_sales2,sat_sales1/sat_sales2
       ,min_week_date1
       ,max_week_date1
       ,max(s_market_desc1) over () as max_market_desc
       ,max(s_company_name1) over () as max_company_name
       ,min(s_city1) over () as min_city_label
 from
 (select s_store_name s_store_name1,wss.d_week_seq d_week_seq1
        ,s_store_id s_store_id1,sun_sales sun_sales1
        ,mon_sales mon_sales1,tue_sales tue_sales1
        ,wed_sales wed_sales1,thu_sales thu_sales1
        ,fri_sales fri_sales1,sat_sales sat_sales1
        ,s_state as s_state1
        ,s_city as s_city1
        ,s_market_desc as s_market_desc1
        ,s_company_name as s_company_name1
        ,min_week_date as min_week_date1
        ,max_week_date as max_week_date1
  from wss,store,date_dim d
  where d.d_week_seq = wss.d_week_seq and
        ss_store_sk = s_store_sk and 
        s_state in ('TN','AL','SD') and
        s_market_desc is not null and
        s_city in ('Oak Grove','Pleasant Hill','Fairview') and
        s_company_name is not null and
        d_month_seq between 1203 and 1203 + 11) y,
 (select s_store_name s_store_name2,wss.d_week_seq d_week_seq2
        ,s_store_id s_store_id2,sun_sales sun_sales2
        ,mon_sales mon_sales2,tue_sales tue_sales2
        ,wed_sales wed_sales2,thu_sales thu_sales2
        ,fri_sales fri_sales2,sat_sales sat_sales2
  from wss,store,date_dim d
 where d.d_week_seq = wss.d_week_seq and
       ss_store_sk = s_store_sk and 
       s_state in ('TN','AL','SD') and
       s_market_desc is not null and
       s_company_name is not null) x
 where s_store_id1=s_store_id2
   and d_week_seq1=d_week_seq2-52
order by d_week_seq1 desc
       ,max_week_date1 desc
       ,min_week_date1 desc

;
