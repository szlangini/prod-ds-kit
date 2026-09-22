select ca_state,
  cd_gender,
  cd_marital_status,
 cd_dep_count,
  count(distinct c_customer_id) as distinct_customer_id_count,
  any_value(c_birth_country) as any_birth_country,
  any_value(ca_city) as any_city_label,
  any_value(c_email_address) as any_email_address,
  max(ca_state) as max_state_label,
  any_value(ca_county) as any_county_label
 from
  customer c,customer_address ca,customer_demographics
where
  c.c_current_addr_sk = ca.ca_address_sk and
  cd_demo_sk = c.c_current_cdemo_sk and 
  ca_country = 'United States' and
  cd_gender in ('F','M') and
  cd_marital_status in ('M','S','D') and
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 2000 and
                d_quarter_name in ('2000Q1','2000Q2','2000Q3')) and
   (exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 2000 and
                  d_quarter_name in ('2000Q1','2000Q2','2000Q3')) or 
    exists (select * 
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2000 and
                  d_quarter_name in ('2000Q1','2000Q2','2000Q3')))
 group by ca_state,
          ca_country,
          ca_county,
          ca_city,
          ca_zip,
          cd_gender,
          cd_marital_status,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 order by distinct_customer_id_count desc,
          cd_dep_count desc,
          cd_dep_employed_count desc,
          cd_dep_college_count desc

;
