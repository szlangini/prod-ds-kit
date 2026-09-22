#!/usr/bin/env python3
"""Whale-query demo: normal vs skewed parameters on key-skewed Prod-DS (SF10).

JCC-H's core empirical claim, reproduced on our data: the SAME query template
with parameters that hit the hot keys ("skewed") vs parameters that avoid them
("normal"). Runs three shapes on DuckDB and reports median runtimes:
  A) scan+join+group (q3-like): store_sales x date_dim x item, month filter
  B) aggregation hotspot: group by customer over a month window
  C) multi-join (q19-like): sales x date x customer x address x store
Parameters are chosen from the data itself: hot = the (d_year, d_moy) of the
dominant sales day / the dominant customer; normal = a median month.
"""
import json
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import duckdb

DATA = Path(sys.argv[1])
OUT = Path(sys.argv[2])
TABLES = ["date_dim", "store_sales", "item", "customer", "customer_address", "store"]
REPS = 3


def load(con):
    schema_sql = (DATA / "_schema.sql").read_text()
    con.execute(schema_sql)
    for t in TABLES:
        f = DATA / f"{t}.dat"
        with tempfile.NamedTemporaryFile(suffix=".dat") as tmp:
            subprocess.run(["iconv", "-f", "ISO-8859-1", "-t", "UTF-8", str(f)],
                           stdout=tmp, check=True)
            tmp.flush()
            con.execute(f"COPY {t} FROM '{tmp.name}' "
                        "(DELIMITER '|', HEADER false, NULL '', AUTO_DETECT false)")
        print(f"[demo] loaded {t}", flush=True)


def timed(con, sql):
    times = []
    rows = 0
    for _ in range(REPS):
        start = time.perf_counter()
        rows = len(con.execute(sql).fetchall())
        times.append(time.perf_counter() - start)
    return round(statistics.median(times), 3), rows


def main():
    con = duckdb.connect()
    con.execute("SET threads TO 8")
    load(con)

    months = con.execute("""
        SELECT d_year, d_moy, count(*) AS c
        FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk
        GROUP BY d_year, d_moy ORDER BY c DESC
    """).fetchall()
    hot_year, hot_moy, hot_rows = months[0]
    mid = months[len(months) // 2]
    normal_year, normal_moy, normal_rows = mid
    print(f"[demo] hot month: {hot_year}-{hot_moy:02d} ({hot_rows} rows) | "
          f"normal month: {normal_year}-{normal_moy:02d} ({normal_rows} rows)", flush=True)

    queries = {
        "A_scan_join_group": """
            SELECT i_brand, count(*) AS n, sum(ss_ext_sales_price) AS rev
            FROM store_sales
            JOIN date_dim ON ss_sold_date_sk = d_date_sk
            JOIN item ON ss_item_sk = i_item_sk
            WHERE d_year = {y} AND d_moy = {m}
            GROUP BY i_brand ORDER BY rev DESC NULLS LAST LIMIT 100
        """,
        "B_agg_hotspot": """
            SELECT ss_customer_sk, count(*) AS n, sum(ss_net_paid) AS paid
            FROM store_sales
            WHERE ss_sold_date_sk IN
                (SELECT d_date_sk FROM date_dim WHERE d_year = {y} AND d_moy = {m})
            GROUP BY ss_customer_sk ORDER BY n DESC NULLS LAST LIMIT 10
        """,
        "C_multi_join": """
            SELECT ca_state, s_store_name, count(*) AS n
            FROM store_sales
            JOIN date_dim ON ss_sold_date_sk = d_date_sk
            JOIN customer ON ss_customer_sk = c_customer_sk
            JOIN customer_address ON c_current_addr_sk = ca_address_sk
            JOIN store ON ss_store_sk = s_store_sk
            WHERE d_year = {y} AND d_moy = {m}
            GROUP BY ca_state, s_store_name ORDER BY n DESC NULLS LAST LIMIT 50
        """,
    }

    results = {"hot_month": [hot_year, hot_moy, hot_rows],
               "normal_month": [normal_year, normal_moy, normal_rows],
               "reps": REPS, "queries": {}}
    for name, template in queries.items():
        normal_t, normal_r = timed(con, template.format(y=normal_year, m=normal_moy))
        skewed_t, skewed_r = timed(con, template.format(y=hot_year, m=hot_moy))
        factor = round(skewed_t / normal_t, 1) if normal_t > 0 else None
        results["queries"][name] = {
            "normal_s": normal_t, "normal_rows": normal_r,
            "skewed_s": skewed_t, "skewed_rows": skewed_r,
            "slowdown": factor,
        }
        print(f"[demo] {name}: normal {normal_t}s ({normal_r} rows) | "
              f"skewed {skewed_t}s ({skewed_r} rows) | x{factor}", flush=True)

    OUT.write_text(json.dumps(results, indent=1))
    print(f"[demo] DONE -> {OUT}", flush=True)


if __name__ == "__main__":
    main()
