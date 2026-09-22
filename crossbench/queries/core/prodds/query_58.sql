select a.ca_state state, count(*) cnt
      ,any_value(i.i_category) as any_item_category
      ,count(distinct i.i_product_name) as distinct_product_name_count
      ,max(cast(d.d_date as timestamp)) as max_sold_ts
 from customer_address a
    ,customer c
    ,store_sales s
     ,date_dim d
     ,item i
 where       a.ca_address_sk = c.c_current_addr_sk
	and c.c_customer_sk = s.ss_customer_sk
	and s.ss_sold_date_sk = d.d_date_sk
	and s.ss_item_sk = i.i_item_sk
	and d.d_month_seq = 
 	     (select min(d_month_seq)
 	      from date_dim
               where d_year = 1998
 	        and d_moy = 5 )
	and i.i_brand is not null
        and a.ca_state in ('CA','NY','TX')
        and a.ca_country in ('United States','Canada')
        and c.c_birth_country in ('FAROE ISLANDS','MAYOTTE','MALAYSIA')
        and i.i_color is not null
        and a.ca_city is not null
group by a.ca_state
 having count(*) >= 1
 order by max_sold_ts desc, cnt desc, distinct_product_name_count desc

;
