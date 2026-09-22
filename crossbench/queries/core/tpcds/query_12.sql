with customer_total_return as materialized (select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_RETURN_AMT_INC_TAX) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =1999
group by sr_customer_sk
,sr_store_sk)
 select  c_customer_id
from customer_total_return ctr1
,(select ctr_store_sk, avg(ctr_total_return)*1.2 as avg_total_return
  from customer_total_return
  group by ctr_store_sk) ctr_avg
,store
,customer
where ctr1.ctr_store_sk = ctr_avg.ctr_store_sk
and ctr1.ctr_total_return > ctr_avg.avg_total_return
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'SD'
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
 limit 100

;
