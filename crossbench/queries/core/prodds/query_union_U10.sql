-- start query 1 in stream 0 using template query_union_U10.tpl
WITH base AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state
  FROM store_sales, date_dim, store
  WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND d_year = 1998
)
,
u0001 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         1 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0002 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         2 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0003 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         3 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0004 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         4 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0005 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         5 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0006 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         6 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0007 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         7 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0008 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         8 AS union_branch
  FROM base
  WHERE d_moy = 8
  ),
u0009 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         9 AS union_branch
  FROM base
  WHERE d_moy = 9
  ),
u0010 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         10 AS union_branch
  FROM base
  WHERE d_moy = 10
  )

SELECT * FROM u0001
UNION ALL
SELECT * FROM u0002
UNION ALL
SELECT * FROM u0003
UNION ALL
SELECT * FROM u0004
UNION ALL
SELECT * FROM u0005
UNION ALL
SELECT * FROM u0006
UNION ALL
SELECT * FROM u0007
UNION ALL
SELECT * FROM u0008
UNION ALL
SELECT * FROM u0009
UNION ALL
SELECT * FROM u0010;

-- end query 1 in stream 0 using template query_union_U10.tpl

;
