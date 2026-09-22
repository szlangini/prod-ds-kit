SELECT
  b.*
FROM (
  SELECT
    b.*
    , lod_01.m_01 AS m_01
    , lod_01.fp_lod_01 AS fp_lod_01
    , lod_02.m_02 AS m_02
    , lod_02.fp_lod_02 AS fp_lod_02
    , lod_03.m_03 AS m_03
    , lod_03.fp_lod_03 AS fp_lod_03
    , lod_04.m_04 AS m_04
    , lod_04.fp_lod_04 AS fp_lod_04
    , lod_05.m_05 AS m_05
    , lod_05.fp_lod_05 AS fp_lod_05
    , lod_06.m_06 AS m_06
    , lod_06.fp_lod_06 AS fp_lod_06
    , lod_07.m_07 AS m_07
    , lod_07.fp_lod_07 AS fp_lod_07
    , lod_08.m_08 AS m_08
    , lod_08.fp_lod_08 AS fp_lod_08
    , lod_09.m_09 AS m_09
    , lod_09.fp_lod_09 AS fp_lod_09
    , lod_10.m_10 AS m_10
    , lod_10.fp_lod_10 AS fp_lod_10
    , lod_11.m_11 AS m_11
    , lod_11.fp_lod_11 AS fp_lod_11
    , lod_12.m_12 AS m_12
    , lod_12.fp_lod_12 AS fp_lod_12
    , lod_13.m_13 AS m_13
    , lod_13.fp_lod_13 AS fp_lod_13
    , lod_14.m_14 AS m_14
    , lod_14.fp_lod_14 AS fp_lod_14
    , lod_15.m_15 AS m_15
    , lod_15.fp_lod_15 AS fp_lod_15
    , lod_16.m_16 AS m_16
    , lod_16.fp_lod_16 AS fp_lod_16
    , lod_17.m_17 AS m_17
    , lod_17.fp_lod_17 AS fp_lod_17
  FROM (
    SELECT
      base_src.*,
      COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1001
    FROM (
      SELECT
          -- PK/Join keys
          sr.sr_ticket_number,
          sr.sr_item_sk,

          -- Measure
          sr.sr_return_amt,

          -- Date keys (from date_dim)
          d.d_year          AS returned_year,
          d.d_moy           AS returned_moy,
          d.d_dow           AS returned_dow,
          d.d_week_seq      AS returned_week_seq,

          -- Time (from time_dim)
          t.t_hour,
          t.t_am_pm,
          t.t_shift,

          -- Item
          i.i_brand,
          i.i_category,
          i.i_class,
          i.i_manufact_id,
          i.i_manufact,
          i.i_size,
          i.i_color,

          -- Customer + Demographics
          c.c_customer_id AS customer_id,
          cd.cd_education_status,
          cd.cd_credit_rating,
          cd.cd_dep_count,

          -- Household + Income band
          hd.hd_buy_potential,
          hd.hd_dep_count      AS hd_dep_count,
          hd.hd_vehicle_count,
          ib.ib_income_band_sk AS income_band,  -- robust proxy for band

          -- Address
          ca.ca_country     AS cust_country,
          ca.ca_state       AS cust_state,
          ca.ca_city        AS cust_city,
          ca.ca_county      AS cust_county,
          ca.ca_zip         AS cust_zip,

          -- Store
          s.s_store_name    AS store_name,
          s.s_state         AS store_state,
          s.s_company_name  AS store_company_name,
          s.s_division_name AS store_division_name,

          -- Reason
          r.r_reason_desc   AS reason_desc
        FROM store_returns sr
        LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
        LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
        LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
        LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
        LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
        LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
        LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
        LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
        LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
        LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
    ) base_src
  ) b
  LEFT JOIN (
    SELECT i_brand AS g_01,
           SUM(sr_return_amt) AS m_01
         , SUM(COALESCE(lod_base_01.dim_fp_1101, 0)) AS fp_lod_01
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1101
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_01
    GROUP BY i_brand
  ) lod_01 ON lod_01.g_01 = b.i_brand
  LEFT JOIN (
    SELECT i_category AS g_02,
           COUNT(sr_return_amt) AS m_02
         , SUM(COALESCE(lod_base_02.dim_fp_1102, 0)) AS fp_lod_02
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1102
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_02
    GROUP BY i_category
  ) lod_02 ON lod_02.g_02 = b.i_category
  LEFT JOIN (
    SELECT store_state AS g_03,
           AVG(sr_return_amt) AS m_03
         , SUM(COALESCE(lod_base_03.dim_fp_1103, 0)) AS fp_lod_03
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1103
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_03
    GROUP BY store_state
  ) lod_03 ON lod_03.g_03 = b.store_state
  LEFT JOIN (
    SELECT store_name AS g_04,
           MIN(sr_return_amt) AS m_04
         , SUM(COALESCE(lod_base_04.dim_fp_1104, 0)) AS fp_lod_04
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1104
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_04
    GROUP BY store_name
  ) lod_04 ON lod_04.g_04 = b.store_name
  LEFT JOIN (
    SELECT cust_state AS g_05,
           MAX(sr_return_amt) AS m_05
         , SUM(COALESCE(lod_base_05.dim_fp_1105, 0)) AS fp_lod_05
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1105
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_05
    GROUP BY cust_state
  ) lod_05 ON lod_05.g_05 = b.cust_state
  LEFT JOIN (
    SELECT cd_education_status AS g_06,
           SUM(sr_return_amt) AS m_06
         , SUM(COALESCE(lod_base_06.dim_fp_1106, 0)) AS fp_lod_06
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1106
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_06
    GROUP BY cd_education_status
  ) lod_06 ON lod_06.g_06 = b.cd_education_status
  LEFT JOIN (
    SELECT cd_credit_rating AS g_07,
           COUNT(sr_return_amt) AS m_07
         , SUM(COALESCE(lod_base_07.dim_fp_1107, 0)) AS fp_lod_07
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1107
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_07
    GROUP BY cd_credit_rating
  ) lod_07 ON lod_07.g_07 = b.cd_credit_rating
  LEFT JOIN (
    SELECT cd_dep_count AS g_08,
           AVG(sr_return_amt) AS m_08
         , SUM(COALESCE(lod_base_08.dim_fp_1108, 0)) AS fp_lod_08
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1108
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_08
    GROUP BY cd_dep_count
  ) lod_08 ON lod_08.g_08 = b.cd_dep_count
  LEFT JOIN (
    SELECT hd_buy_potential AS g_09,
           MIN(sr_return_amt) AS m_09
         , SUM(COALESCE(lod_base_09.dim_fp_1109, 0)) AS fp_lod_09
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1109
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_09
    GROUP BY hd_buy_potential
  ) lod_09 ON lod_09.g_09 = b.hd_buy_potential
  LEFT JOIN (
    SELECT hd_dep_count AS g_10,
           MAX(sr_return_amt) AS m_10
         , SUM(COALESCE(lod_base_10.dim_fp_1110, 0)) AS fp_lod_10
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1110
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_10
    GROUP BY hd_dep_count
  ) lod_10 ON lod_10.g_10 = b.hd_dep_count
  LEFT JOIN (
    SELECT hd_vehicle_count AS g_11,
           SUM(sr_return_amt) AS m_11
         , SUM(COALESCE(lod_base_11.dim_fp_1111, 0)) AS fp_lod_11
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1111
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_11
    GROUP BY hd_vehicle_count
  ) lod_11 ON lod_11.g_11 = b.hd_vehicle_count
  LEFT JOIN (
    SELECT income_band AS g_12,
           COUNT(sr_return_amt) AS m_12
         , SUM(COALESCE(lod_base_12.dim_fp_1112, 0)) AS fp_lod_12
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1112
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_12
    GROUP BY income_band
  ) lod_12 ON lod_12.g_12 = b.income_band
  LEFT JOIN (
    SELECT reason_desc AS g_13,
           AVG(sr_return_amt) AS m_13
         , SUM(COALESCE(lod_base_13.dim_fp_1113, 0)) AS fp_lod_13
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1113
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_13
    GROUP BY reason_desc
  ) lod_13 ON lod_13.g_13 = b.reason_desc
  LEFT JOIN (
    SELECT returned_year AS g_14,
           MIN(sr_return_amt) AS m_14
         , SUM(COALESCE(lod_base_14.dim_fp_1114, 0)) AS fp_lod_14
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1114
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_14
    GROUP BY returned_year
  ) lod_14 ON lod_14.g_14 = b.returned_year
  LEFT JOIN (
    SELECT returned_moy AS g_15,
           MAX(sr_return_amt) AS m_15
         , SUM(COALESCE(lod_base_15.dim_fp_1115, 0)) AS fp_lod_15
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1115
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_15
    GROUP BY returned_moy
  ) lod_15 ON lod_15.g_15 = b.returned_moy
  LEFT JOIN (
    SELECT returned_dow AS g_16,
           SUM(sr_return_amt) AS m_16
         , SUM(COALESCE(lod_base_16.dim_fp_1116, 0)) AS fp_lod_16
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1116
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_16
    GROUP BY returned_dow
  ) lod_16 ON lod_16.g_16 = b.returned_dow
  LEFT JOIN (
    SELECT returned_week_seq AS g_17,
           COUNT(sr_return_amt) AS m_17
         , SUM(COALESCE(lod_base_17.dim_fp_1117, 0)) AS fp_lod_17
    FROM (
      SELECT
        base_src.*,
        COALESCE(LENGTH(CAST(base_src.returned_year AS VARCHAR)), 0) * 1 + COALESCE(LENGTH(CAST(base_src.t_hour AS VARCHAR)), 0) * 2 + COALESCE(LENGTH(CAST(base_src.i_brand AS VARCHAR)), 0) * 3 + COALESCE(LENGTH(CAST(base_src.customer_id AS VARCHAR)), 0) * 4 + COALESCE(LENGTH(CAST(base_src.cd_dep_count AS VARCHAR)), 0) * 5 + COALESCE(LENGTH(CAST(base_src.hd_vehicle_count AS VARCHAR)), 0) * 6 + COALESCE(LENGTH(CAST(base_src.income_band AS VARCHAR)), 0) * 7 + COALESCE(LENGTH(CAST(base_src.cust_zip AS VARCHAR)), 0) * 8 + COALESCE(LENGTH(CAST(base_src.store_name AS VARCHAR)), 0) * 9 + COALESCE(LENGTH(CAST(base_src.reason_desc AS VARCHAR)), 0) * 10 AS dim_fp_1117
      FROM (
        SELECT
            -- PK/Join keys
            sr.sr_ticket_number,
            sr.sr_item_sk,

            -- Measure
            sr.sr_return_amt,

            -- Date keys (from date_dim)
            d.d_year          AS returned_year,
            d.d_moy           AS returned_moy,
            d.d_dow           AS returned_dow,
            d.d_week_seq      AS returned_week_seq,

            -- Time (from time_dim)
            t.t_hour,
            t.t_am_pm,
            t.t_shift,

            -- Item
            i.i_brand,
            i.i_category,
            i.i_class,
            i.i_manufact_id,
            i.i_manufact,
            i.i_size,
            i.i_color,

            -- Customer + Demographics
            c.c_customer_id AS customer_id,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_dep_count,

            -- Household + Income band
            hd.hd_buy_potential,
            hd.hd_dep_count      AS hd_dep_count,
            hd.hd_vehicle_count,
            ib.ib_income_band_sk AS income_band,  -- robust proxy for band

            -- Address
            ca.ca_country     AS cust_country,
            ca.ca_state       AS cust_state,
            ca.ca_city        AS cust_city,
            ca.ca_county      AS cust_county,
            ca.ca_zip         AS cust_zip,

            -- Store
            s.s_store_name    AS store_name,
            s.s_state         AS store_state,
            s.s_company_name  AS store_company_name,
            s.s_division_name AS store_division_name,

            -- Reason
            r.r_reason_desc   AS reason_desc
          FROM store_returns sr
          LEFT JOIN date_dim               d  ON d.d_date_sk          = sr.sr_returned_date_sk
          LEFT JOIN time_dim               t  ON t.t_time_sk          = sr.sr_return_time_sk
          LEFT JOIN item                   i  ON i.i_item_sk          = sr.sr_item_sk
          LEFT JOIN customer               c  ON c.c_customer_sk      = sr.sr_customer_sk
          LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = sr.sr_cdemo_sk
          LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = sr.sr_hdemo_sk
          LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
          LEFT JOIN customer_address       ca ON ca.ca_address_sk     = sr.sr_addr_sk
          LEFT JOIN store                  s  ON s.s_store_sk         = sr.sr_store_sk
          LEFT JOIN reason                 r  ON r.r_reason_sk        = sr.sr_reason_sk
      ) base_src
    ) lod_base_17
    GROUP BY returned_week_seq
  ) lod_17 ON lod_17.g_17 = b.returned_week_seq
) b

;
