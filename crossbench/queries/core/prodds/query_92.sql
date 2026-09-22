with ssales as
(select c_last_name
      ,c_first_name
      ,c_birth_country
      ,s_store_name
      ,ca_state
      ,s_state
      ,i_color
      ,i_current_price
      ,i_manager_id
      ,i_units
      ,i_size
      ,sum(ss_net_paid) netpaid
from store_sales
    ,store_returns
    ,store
    ,item
    ,customer
    ,customer_address
where ss_ticket_number = sr_ticket_number
  and ss_item_sk = sr_item_sk
  and ss_customer_sk = c_customer_sk
  and ss_item_sk = i_item_sk
  and ss_store_sk = s_store_sk
  and c_current_addr_sk = ca_address_sk
  and c_birth_country <> upper(ca_country)
  and ca_country = 'United States'
  and c_preferred_cust_flag = 'Y'
  and s_zip = ca_zip
  and s_state in ('TN','SD','AL')
  and i_units in ('Each','Box','Case')
  and i_color is not null
group by c_last_name
        ,c_first_name
        ,c_birth_country
        ,s_store_name
        ,ca_state
        ,s_state
        ,i_color
        ,i_current_price
        ,i_manager_id
        ,i_units
        ,i_size)
select c_last_name
      ,c_first_name
      ,s_store_name
      ,sum(netpaid) paid
      ,any_value(ca_state) as any_birth_country
      ,any_value(i_color) as any_item_color
      ,count(distinct ca_state) as distinct_state_count
from ssales
where i_color = 'green'
group by c_last_name
        ,c_first_name
        ,s_store_name
having sum(netpaid) > 0
order by paid desc

;
