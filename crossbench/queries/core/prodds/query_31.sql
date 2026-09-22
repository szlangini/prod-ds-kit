select i_item_id
      ,i_item_desc
      ,i_current_price
      ,avg(inv_quantity_on_hand) as avg_inv_qty
      ,any_value(i_category) as any_item_category
      ,max(i_brand) as max_item_brand
      ,count(distinct i_product_name) as distinct_product_name_count
      ,any_value(i_class) as any_item_class
      ,min(d_date) as min_inv_date
      ,max(cast(d_date as timestamp)) as max_inv_ts
 from item, inventory, date_dim, catalog_sales
 where i_current_price between 11 and 11 + 30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('1999-03-30' as date) and (cast('1999-03-30' as date) + interval '60 days')
 and i_manufact_id in (930,728,982,757)
 and i_category in ('Books','Electronics','Sports')
 and i_class is not null
 and i_color is not null
 and i_units in ('Each','Box','Case')
 and cs_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by avg_inv_qty desc
         ,max_inv_ts desc
         ,min_inv_date desc

;
