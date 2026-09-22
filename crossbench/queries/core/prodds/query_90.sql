select i_item_id,
        ca_country,
        ca_state, 
        ca_county,
       avg( cast(cs_sales_price as decimal(12,2))) agg4,
        avg( cast(cs_net_profit as decimal(12,2))) agg5,
        any_value(i_item_desc) as any_item_desc,
        any_value(i_category) as any_item_category,
        count(distinct ca_city) as distinct_city_count,
        any_value(i_brand) as any_item_brand,
        max(i_product_name) as max_product_name,
        count(distinct c_email_address) as distinct_email_count,
        any_value(cd1.cd_education_status) as any_bill_education_status,
        max(ca_city) as max_city_label,
        min(d_date) as min_sold_date,
        max(cast(d_date as timestamp)) as max_sold_ts
 from catalog_sales, customer_demographics cd1, 
      customer_demographics cd2, customer, customer_address, date_dim, item
 where cs_sold_date_sk = d_date_sk and
       cs_item_sk = i_item_sk and
       cs_bill_cdemo_sk = cd1.cd_demo_sk and
       cs_bill_customer_sk = c_customer_sk and
       cd1.cd_gender = 'M' and 
       cd1.cd_education_status = 'College' and
       c_current_cdemo_sk = cd2.cd_demo_sk and
       c_current_addr_sk = ca_address_sk and
       d_year = 2001 and
       ca_country = 'United States' and
       ca_state in ('UT','ND','TX'
                   ,'IN','MO','MT','KY')
       and cd2.cd_marital_status in ('M','S')
       and ca_city in ('Utica','Melrose','Hopewell')
       and i_category in ('Home','Electronics','Sports')
 group by rollup (i_item_id, ca_country, ca_state, ca_county)
 order by max_sold_ts desc,
        agg5 desc,
        agg4 desc

;
