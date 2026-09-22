select distinct(i_product_name)
        ,max(i_brand) over () as max_brand
        ,any_value(i_category) over () as any_category
        ,max(i_manufact) over () as max_manufact
        ,min(i_color) over () as min_color
        ,any_value(i_class) over () as any_class
 from item i1
 where i_category in ('Women','Men')
   and i_color is not null
   and i_product_name is not null
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'midnight' or i_color = 'powder') and 
        (i_units = 'Box' or i_units = 'Gram') and
        (i_size = 'petite' or i_size = 'large')
        ) or
        (i_category = 'Women' and
        (i_color = 'honeydew' or i_color = 'hot') and
        (i_units = 'N/A' or i_units = 'Cup') and
        (i_size = 'small' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'azure' or i_color = 'peru') and
        (i_units = 'Carton' or i_units = 'Tsp') and
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'tan' or i_color = 'navy') and
        (i_units = 'Ounce' or i_units = 'Gross') and
        (i_size = 'petite' or i_size = 'large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'lace' or i_color = 'saddle') and 
        (i_units = 'Tbl' or i_units = 'Bunch') and
        (i_size = 'petite' or i_size = 'large')
        ) or
        (i_category = 'Women' and
        (i_color = 'dodger' or i_color = 'burnished') and
        (i_units = 'Ton' or i_units = 'Dram') and
        (i_size = 'small' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'deep' or i_color = 'chocolate') and
        (i_units = 'Pallet' or i_units = 'Each') and
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'orchid' or i_color = 'rosy') and
        (i_units = 'Unknown' or i_units = 'Oz') and
        (i_size = 'petite' or i_size = 'large')
        )))) > 0
 order by i_product_name desc

;
