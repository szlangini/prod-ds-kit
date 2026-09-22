select  *
from (select /*KEEP_LIMIT*/ avg(ss_list_price) B1_LP
            ,count(distinct ss_ticket_number) as B1_ticket_count
            ,max(ss_ticket_number) as B1_max_ticket
            ,min(ss_ticket_number) as B1_min_ticket
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 82 and 82+10 
             or ss_coupon_amt between 9222 and 9222+1000
             or ss_wholesale_cost between 64 and 64+20) limit 10) B1,
     (select avg(ss_list_price) B2_LP
            ,count(distinct ss_ticket_number) as B2_ticket_count
            ,max(ss_ticket_number) as B2_max_ticket
            ,min(ss_ticket_number) as B2_min_ticket
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 106 and 106+10
          or ss_coupon_amt between 9372 and 9372+1000
          or ss_wholesale_cost between 7 and 7+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(distinct ss_ticket_number) as B3_ticket_count
            ,max(ss_ticket_number) as B3_max_ticket
            ,min(ss_ticket_number) as B3_min_ticket
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 77 and 77+10
          or ss_coupon_amt between 5174 and 5174+1000
          or ss_wholesale_cost between 5 and 5+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(distinct ss_ticket_number) as B4_ticket_count
            ,max(ss_ticket_number) as B4_max_ticket
            ,min(ss_ticket_number) as B4_min_ticket
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 0 and 0+10
          or ss_coupon_amt between 17802 and 17802+1000
          or ss_wholesale_cost between 54 and 54+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(distinct ss_ticket_number) as B5_ticket_count
            ,max(ss_ticket_number) as B5_max_ticket
            ,min(ss_ticket_number) as B5_min_ticket
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 64 and 64+10
          or ss_coupon_amt between 2706 and 2706+1000
          or ss_wholesale_cost between 68 and 68+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(distinct ss_ticket_number) as B6_ticket_count
            ,max(ss_ticket_number) as B6_max_ticket
            ,min(ss_ticket_number) as B6_min_ticket
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 59 and 59+10
          or ss_coupon_amt between 7570 and 7570+1000
          or ss_wholesale_cost between 16 and 16+20)) B6
 limit 500000

;
