-- start query 1 in stream 0 using template query_union_U200.tpl
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
  ),
u0011 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         11 AS union_branch
  FROM base
  WHERE d_moy = 11
  ),
u0012 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         12 AS union_branch
  FROM base
  WHERE d_moy = 12
  ),
u0013 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         13 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0014 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         14 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0015 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         15 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0016 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         16 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0017 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         17 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0018 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         18 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0019 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         19 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0020 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         20 AS union_branch
  FROM base
  WHERE d_moy = 8
  ),
u0021 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         21 AS union_branch
  FROM base
  WHERE d_moy = 9
  ),
u0022 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         22 AS union_branch
  FROM base
  WHERE d_moy = 10
  ),
u0023 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         23 AS union_branch
  FROM base
  WHERE d_moy = 11
  ),
u0024 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         24 AS union_branch
  FROM base
  WHERE d_moy = 12
  ),
u0025 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         25 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0026 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         26 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0027 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         27 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0028 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         28 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0029 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         29 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0030 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         30 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0031 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         31 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0032 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         32 AS union_branch
  FROM base
  WHERE d_moy = 8
  ),
u0033 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         33 AS union_branch
  FROM base
  WHERE d_moy = 9
  ),
u0034 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         34 AS union_branch
  FROM base
  WHERE d_moy = 10
  ),
u0035 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         35 AS union_branch
  FROM base
  WHERE d_moy = 11
  ),
u0036 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         36 AS union_branch
  FROM base
  WHERE d_moy = 12
  ),
u0037 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         37 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0038 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         38 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0039 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         39 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0040 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         40 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0041 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         41 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0042 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         42 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0043 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         43 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0044 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         44 AS union_branch
  FROM base
  WHERE d_moy = 8
  ),
u0045 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         45 AS union_branch
  FROM base
  WHERE d_moy = 9
  ),
u0046 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         46 AS union_branch
  FROM base
  WHERE d_moy = 10
  ),
u0047 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         47 AS union_branch
  FROM base
  WHERE d_moy = 11
  ),
u0048 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         48 AS union_branch
  FROM base
  WHERE d_moy = 12
  ),
u0049 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         49 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0050 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         50 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0051 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         51 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0052 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         52 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0053 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         53 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0054 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         54 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0055 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         55 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0056 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         56 AS union_branch
  FROM base
  WHERE d_moy = 8
  ),
u0057 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         57 AS union_branch
  FROM base
  WHERE d_moy = 9
  ),
u0058 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         58 AS union_branch
  FROM base
  WHERE d_moy = 10
  ),
u0059 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         59 AS union_branch
  FROM base
  WHERE d_moy = 11
  ),
u0060 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         60 AS union_branch
  FROM base
  WHERE d_moy = 12
  ),
u0061 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         61 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0062 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         62 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0063 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         63 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0064 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         64 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0065 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         65 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0066 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         66 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0067 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         67 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0068 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         68 AS union_branch
  FROM base
  WHERE d_moy = 8
  ),
u0069 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         69 AS union_branch
  FROM base
  WHERE d_moy = 9
  ),
u0070 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         70 AS union_branch
  FROM base
  WHERE d_moy = 10
  ),
u0071 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         71 AS union_branch
  FROM base
  WHERE d_moy = 11
  ),
u0072 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         72 AS union_branch
  FROM base
  WHERE d_moy = 12
  ),
u0073 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         73 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0074 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         74 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0075 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         75 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0076 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         76 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0077 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         77 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0078 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         78 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0079 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         79 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0080 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         80 AS union_branch
  FROM base
  WHERE d_moy = 8
  ),
u0081 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         81 AS union_branch
  FROM base
  WHERE d_moy = 9
  ),
u0082 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         82 AS union_branch
  FROM base
  WHERE d_moy = 10
  ),
u0083 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         83 AS union_branch
  FROM base
  WHERE d_moy = 11
  ),
u0084 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         84 AS union_branch
  FROM base
  WHERE d_moy = 12
  ),
u0085 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         85 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0086 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         86 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0087 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         87 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0088 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         88 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0089 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         89 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0090 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         90 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0091 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         91 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0092 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         92 AS union_branch
  FROM base
  WHERE d_moy = 8
  ),
u0093 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         93 AS union_branch
  FROM base
  WHERE d_moy = 9
  ),
u0094 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         94 AS union_branch
  FROM base
  WHERE d_moy = 10
  ),
u0095 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         95 AS union_branch
  FROM base
  WHERE d_moy = 11
  ),
u0096 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         96 AS union_branch
  FROM base
  WHERE d_moy = 12
  ),
u0097 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         97 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0098 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         98 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0099 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         99 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0100 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         100 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0101 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         101 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0102 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         102 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0103 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         103 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0104 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         104 AS union_branch
  FROM base
  WHERE d_moy = 8
  ),
u0105 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         105 AS union_branch
  FROM base
  WHERE d_moy = 9
  ),
u0106 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         106 AS union_branch
  FROM base
  WHERE d_moy = 10
  ),
u0107 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         107 AS union_branch
  FROM base
  WHERE d_moy = 11
  ),
u0108 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         108 AS union_branch
  FROM base
  WHERE d_moy = 12
  ),
u0109 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         109 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0110 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         110 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0111 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         111 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0112 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         112 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0113 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         113 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0114 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         114 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0115 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         115 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0116 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         116 AS union_branch
  FROM base
  WHERE d_moy = 8
  ),
u0117 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         117 AS union_branch
  FROM base
  WHERE d_moy = 9
  ),
u0118 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         118 AS union_branch
  FROM base
  WHERE d_moy = 10
  ),
u0119 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         119 AS union_branch
  FROM base
  WHERE d_moy = 11
  ),
u0120 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         120 AS union_branch
  FROM base
  WHERE d_moy = 12
  ),
u0121 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         121 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0122 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         122 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0123 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         123 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0124 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         124 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0125 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         125 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0126 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         126 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0127 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         127 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0128 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         128 AS union_branch
  FROM base
  WHERE d_moy = 8
  ),
u0129 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         129 AS union_branch
  FROM base
  WHERE d_moy = 9
  ),
u0130 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         130 AS union_branch
  FROM base
  WHERE d_moy = 10
  ),
u0131 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         131 AS union_branch
  FROM base
  WHERE d_moy = 11
  ),
u0132 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         132 AS union_branch
  FROM base
  WHERE d_moy = 12
  ),
u0133 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         133 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0134 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         134 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0135 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         135 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0136 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         136 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0137 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         137 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0138 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         138 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0139 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         139 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0140 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         140 AS union_branch
  FROM base
  WHERE d_moy = 8
  ),
u0141 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         141 AS union_branch
  FROM base
  WHERE d_moy = 9
  ),
u0142 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         142 AS union_branch
  FROM base
  WHERE d_moy = 10
  ),
u0143 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         143 AS union_branch
  FROM base
  WHERE d_moy = 11
  ),
u0144 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         144 AS union_branch
  FROM base
  WHERE d_moy = 12
  ),
u0145 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         145 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0146 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         146 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0147 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         147 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0148 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         148 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0149 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         149 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0150 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         150 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0151 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         151 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0152 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         152 AS union_branch
  FROM base
  WHERE d_moy = 8
  ),
u0153 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         153 AS union_branch
  FROM base
  WHERE d_moy = 9
  ),
u0154 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         154 AS union_branch
  FROM base
  WHERE d_moy = 10
  ),
u0155 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         155 AS union_branch
  FROM base
  WHERE d_moy = 11
  ),
u0156 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         156 AS union_branch
  FROM base
  WHERE d_moy = 12
  ),
u0157 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         157 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0158 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         158 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0159 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         159 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0160 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         160 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0161 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         161 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0162 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         162 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0163 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         163 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0164 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         164 AS union_branch
  FROM base
  WHERE d_moy = 8
  ),
u0165 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         165 AS union_branch
  FROM base
  WHERE d_moy = 9
  ),
u0166 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         166 AS union_branch
  FROM base
  WHERE d_moy = 10
  ),
u0167 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         167 AS union_branch
  FROM base
  WHERE d_moy = 11
  ),
u0168 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         168 AS union_branch
  FROM base
  WHERE d_moy = 12
  ),
u0169 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         169 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0170 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         170 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0171 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         171 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0172 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         172 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0173 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         173 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0174 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         174 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0175 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         175 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0176 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         176 AS union_branch
  FROM base
  WHERE d_moy = 8
  ),
u0177 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         177 AS union_branch
  FROM base
  WHERE d_moy = 9
  ),
u0178 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         178 AS union_branch
  FROM base
  WHERE d_moy = 10
  ),
u0179 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         179 AS union_branch
  FROM base
  WHERE d_moy = 11
  ),
u0180 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         180 AS union_branch
  FROM base
  WHERE d_moy = 12
  ),
u0181 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         181 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0182 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         182 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0183 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         183 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0184 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         184 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0185 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         185 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0186 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         186 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0187 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         187 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0188 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         188 AS union_branch
  FROM base
  WHERE d_moy = 8
  ),
u0189 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         189 AS union_branch
  FROM base
  WHERE d_moy = 9
  ),
u0190 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         190 AS union_branch
  FROM base
  WHERE d_moy = 10
  ),
u0191 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         191 AS union_branch
  FROM base
  WHERE d_moy = 11
  ),
u0192 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         192 AS union_branch
  FROM base
  WHERE d_moy = 12
  ),
u0193 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         193 AS union_branch
  FROM base
  WHERE d_moy = 1
  ),
u0194 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         194 AS union_branch
  FROM base
  WHERE d_moy = 2
  ),
u0195 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         195 AS union_branch
  FROM base
  WHERE d_moy = 3
  ),
u0196 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         196 AS union_branch
  FROM base
  WHERE d_moy = 4
  ),
u0197 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         197 AS union_branch
  FROM base
  WHERE d_moy = 5
  ),
u0198 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         198 AS union_branch
  FROM base
  WHERE d_moy = 6
  ),
u0199 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         199 AS union_branch
  FROM base
  WHERE d_moy = 7
  ),
u0200 AS (
  SELECT ss_item_sk,
         ss_ext_sales_price,
         d_year,
         d_moy,
         s_state,
         200 AS union_branch
  FROM base
  WHERE d_moy = 8
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
SELECT * FROM u0010
UNION ALL
SELECT * FROM u0011
UNION ALL
SELECT * FROM u0012
UNION ALL
SELECT * FROM u0013
UNION ALL
SELECT * FROM u0014
UNION ALL
SELECT * FROM u0015
UNION ALL
SELECT * FROM u0016
UNION ALL
SELECT * FROM u0017
UNION ALL
SELECT * FROM u0018
UNION ALL
SELECT * FROM u0019
UNION ALL
SELECT * FROM u0020
UNION ALL
SELECT * FROM u0021
UNION ALL
SELECT * FROM u0022
UNION ALL
SELECT * FROM u0023
UNION ALL
SELECT * FROM u0024
UNION ALL
SELECT * FROM u0025
UNION ALL
SELECT * FROM u0026
UNION ALL
SELECT * FROM u0027
UNION ALL
SELECT * FROM u0028
UNION ALL
SELECT * FROM u0029
UNION ALL
SELECT * FROM u0030
UNION ALL
SELECT * FROM u0031
UNION ALL
SELECT * FROM u0032
UNION ALL
SELECT * FROM u0033
UNION ALL
SELECT * FROM u0034
UNION ALL
SELECT * FROM u0035
UNION ALL
SELECT * FROM u0036
UNION ALL
SELECT * FROM u0037
UNION ALL
SELECT * FROM u0038
UNION ALL
SELECT * FROM u0039
UNION ALL
SELECT * FROM u0040
UNION ALL
SELECT * FROM u0041
UNION ALL
SELECT * FROM u0042
UNION ALL
SELECT * FROM u0043
UNION ALL
SELECT * FROM u0044
UNION ALL
SELECT * FROM u0045
UNION ALL
SELECT * FROM u0046
UNION ALL
SELECT * FROM u0047
UNION ALL
SELECT * FROM u0048
UNION ALL
SELECT * FROM u0049
UNION ALL
SELECT * FROM u0050
UNION ALL
SELECT * FROM u0051
UNION ALL
SELECT * FROM u0052
UNION ALL
SELECT * FROM u0053
UNION ALL
SELECT * FROM u0054
UNION ALL
SELECT * FROM u0055
UNION ALL
SELECT * FROM u0056
UNION ALL
SELECT * FROM u0057
UNION ALL
SELECT * FROM u0058
UNION ALL
SELECT * FROM u0059
UNION ALL
SELECT * FROM u0060
UNION ALL
SELECT * FROM u0061
UNION ALL
SELECT * FROM u0062
UNION ALL
SELECT * FROM u0063
UNION ALL
SELECT * FROM u0064
UNION ALL
SELECT * FROM u0065
UNION ALL
SELECT * FROM u0066
UNION ALL
SELECT * FROM u0067
UNION ALL
SELECT * FROM u0068
UNION ALL
SELECT * FROM u0069
UNION ALL
SELECT * FROM u0070
UNION ALL
SELECT * FROM u0071
UNION ALL
SELECT * FROM u0072
UNION ALL
SELECT * FROM u0073
UNION ALL
SELECT * FROM u0074
UNION ALL
SELECT * FROM u0075
UNION ALL
SELECT * FROM u0076
UNION ALL
SELECT * FROM u0077
UNION ALL
SELECT * FROM u0078
UNION ALL
SELECT * FROM u0079
UNION ALL
SELECT * FROM u0080
UNION ALL
SELECT * FROM u0081
UNION ALL
SELECT * FROM u0082
UNION ALL
SELECT * FROM u0083
UNION ALL
SELECT * FROM u0084
UNION ALL
SELECT * FROM u0085
UNION ALL
SELECT * FROM u0086
UNION ALL
SELECT * FROM u0087
UNION ALL
SELECT * FROM u0088
UNION ALL
SELECT * FROM u0089
UNION ALL
SELECT * FROM u0090
UNION ALL
SELECT * FROM u0091
UNION ALL
SELECT * FROM u0092
UNION ALL
SELECT * FROM u0093
UNION ALL
SELECT * FROM u0094
UNION ALL
SELECT * FROM u0095
UNION ALL
SELECT * FROM u0096
UNION ALL
SELECT * FROM u0097
UNION ALL
SELECT * FROM u0098
UNION ALL
SELECT * FROM u0099
UNION ALL
SELECT * FROM u0100
UNION ALL
SELECT * FROM u0101
UNION ALL
SELECT * FROM u0102
UNION ALL
SELECT * FROM u0103
UNION ALL
SELECT * FROM u0104
UNION ALL
SELECT * FROM u0105
UNION ALL
SELECT * FROM u0106
UNION ALL
SELECT * FROM u0107
UNION ALL
SELECT * FROM u0108
UNION ALL
SELECT * FROM u0109
UNION ALL
SELECT * FROM u0110
UNION ALL
SELECT * FROM u0111
UNION ALL
SELECT * FROM u0112
UNION ALL
SELECT * FROM u0113
UNION ALL
SELECT * FROM u0114
UNION ALL
SELECT * FROM u0115
UNION ALL
SELECT * FROM u0116
UNION ALL
SELECT * FROM u0117
UNION ALL
SELECT * FROM u0118
UNION ALL
SELECT * FROM u0119
UNION ALL
SELECT * FROM u0120
UNION ALL
SELECT * FROM u0121
UNION ALL
SELECT * FROM u0122
UNION ALL
SELECT * FROM u0123
UNION ALL
SELECT * FROM u0124
UNION ALL
SELECT * FROM u0125
UNION ALL
SELECT * FROM u0126
UNION ALL
SELECT * FROM u0127
UNION ALL
SELECT * FROM u0128
UNION ALL
SELECT * FROM u0129
UNION ALL
SELECT * FROM u0130
UNION ALL
SELECT * FROM u0131
UNION ALL
SELECT * FROM u0132
UNION ALL
SELECT * FROM u0133
UNION ALL
SELECT * FROM u0134
UNION ALL
SELECT * FROM u0135
UNION ALL
SELECT * FROM u0136
UNION ALL
SELECT * FROM u0137
UNION ALL
SELECT * FROM u0138
UNION ALL
SELECT * FROM u0139
UNION ALL
SELECT * FROM u0140
UNION ALL
SELECT * FROM u0141
UNION ALL
SELECT * FROM u0142
UNION ALL
SELECT * FROM u0143
UNION ALL
SELECT * FROM u0144
UNION ALL
SELECT * FROM u0145
UNION ALL
SELECT * FROM u0146
UNION ALL
SELECT * FROM u0147
UNION ALL
SELECT * FROM u0148
UNION ALL
SELECT * FROM u0149
UNION ALL
SELECT * FROM u0150
UNION ALL
SELECT * FROM u0151
UNION ALL
SELECT * FROM u0152
UNION ALL
SELECT * FROM u0153
UNION ALL
SELECT * FROM u0154
UNION ALL
SELECT * FROM u0155
UNION ALL
SELECT * FROM u0156
UNION ALL
SELECT * FROM u0157
UNION ALL
SELECT * FROM u0158
UNION ALL
SELECT * FROM u0159
UNION ALL
SELECT * FROM u0160
UNION ALL
SELECT * FROM u0161
UNION ALL
SELECT * FROM u0162
UNION ALL
SELECT * FROM u0163
UNION ALL
SELECT * FROM u0164
UNION ALL
SELECT * FROM u0165
UNION ALL
SELECT * FROM u0166
UNION ALL
SELECT * FROM u0167
UNION ALL
SELECT * FROM u0168
UNION ALL
SELECT * FROM u0169
UNION ALL
SELECT * FROM u0170
UNION ALL
SELECT * FROM u0171
UNION ALL
SELECT * FROM u0172
UNION ALL
SELECT * FROM u0173
UNION ALL
SELECT * FROM u0174
UNION ALL
SELECT * FROM u0175
UNION ALL
SELECT * FROM u0176
UNION ALL
SELECT * FROM u0177
UNION ALL
SELECT * FROM u0178
UNION ALL
SELECT * FROM u0179
UNION ALL
SELECT * FROM u0180
UNION ALL
SELECT * FROM u0181
UNION ALL
SELECT * FROM u0182
UNION ALL
SELECT * FROM u0183
UNION ALL
SELECT * FROM u0184
UNION ALL
SELECT * FROM u0185
UNION ALL
SELECT * FROM u0186
UNION ALL
SELECT * FROM u0187
UNION ALL
SELECT * FROM u0188
UNION ALL
SELECT * FROM u0189
UNION ALL
SELECT * FROM u0190
UNION ALL
SELECT * FROM u0191
UNION ALL
SELECT * FROM u0192
UNION ALL
SELECT * FROM u0193
UNION ALL
SELECT * FROM u0194
UNION ALL
SELECT * FROM u0195
UNION ALL
SELECT * FROM u0196
UNION ALL
SELECT * FROM u0197
UNION ALL
SELECT * FROM u0198
UNION ALL
SELECT * FROM u0199
UNION ALL
SELECT * FROM u0200;

-- end query 1 in stream 0 using template query_union_U200.tpl

;
