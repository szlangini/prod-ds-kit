select cd_gender,
  cd_marital_status,
  cd_education_status,
  cd_purchase_estimate,
  cd_credit_rating,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count,
  any_value(c_birth_country) as any_birth_country,
  any_value(ca_state) as any_state_label,
  count(distinct c_customer_id) as distinct_customer_id_count,
  any_value(c_first_name) as any_first_name,
  any_value(c_last_name) as any_last_name,
  any_value(c_email_address) as any_email_address,
  min(ca_city) as min_city_label,
  max(ca_county) as max_county_label
 from
  customer c,customer_address ca,customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_county in ('Hood County','Geary County','Shelby County','Hancock County','Sumner County') and
  cd_demo_sk = c.c_current_cdemo_sk and 
  cd_gender in ('F','M') and
  cd_marital_status in ('S','M','D') and
  cd_credit_rating in ('Good','Excellent','Unknown') and
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 1999 and
                d_day_name in ('Monday','Tuesday','Wednesday','Thursday')) and
   (exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 1999 and
                  d_day_name in ('Monday','Tuesday','Wednesday','Thursday')) or 
    exists (select * 
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 1999 and
                  d_day_name in ('Monday','Tuesday','Wednesday','Thursday')))
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count,
          ca_state,
          ca_city,
          ca_zip
 order by distinct_customer_id_count desc,
  cd_dep_count desc,
  cd_dep_employed_count desc,
  cd_dep_college_count desc

;
