select * from 
(select i_manufact_id,
sum(ss_sales_price) sum_sales,
avg(sum(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales,
any_value(i_category) as any_category,
max(i_product_name) as max_product_name,
count(distinct s_city) as distinct_store_city_count,
any_value(s_company_name) as any_company_name,
min(d_date) as min_sold_date,
max(cast(d_date as timestamp)) as max_sold_ts
from item, store_sales, date_dim, store
where ss_item_sk = i_item_sk and
ss_sold_date_sk = d_date_sk and
ss_store_sk = s_store_sk and
d_month_seq in (1206,1206+1,1206+2,1206+3,1206+4,1206+5,1206+6,1206+7,1206+8,1206+9,1206+10,1206+11) and
((i_category in ('Books','Children','Electronics') and
i_class in ('personal','portable','reference','self-help') and
i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
		'exportiunivamalg #9','scholaramalgamalg #9'))
or(i_category in ('Women','Music','Men') and
i_class in ('accessories','classical','fragrances','pants') and
i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
		'importoamalg #1')))
and s_state in ('SD','TN','AL')
and i_brand is not null
and s_company_name is not null
group by i_manufact_id, d_qoy ) tmp1
where case when avg_quarterly_sales > 0 
	then abs (sum_sales - avg_quarterly_sales)/ avg_quarterly_sales 
	else null end > 0.0
order by avg_quarterly_sales,
	 sum_sales,
	 i_manufact_id

;
