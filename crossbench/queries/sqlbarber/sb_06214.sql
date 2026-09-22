SELECT m.movie_id,
       COUNT(c.id) AS cast_info_count, -- Aggregation 1
 MIN(t.production_year) AS min_production_year, -- Aggregation 2
 MAX(t.production_year) AS max_production_year, -- Aggregation 3
 AVG(r.id) AS avg_role_id, -- Aggregation 4

  (SELECT SUM(mc2.company_id) -- Aggregation 5 via nested query

   FROM movie_companies mc2
   WHERE mc2.movie_id = m.movie_id) AS nested_sum_company_id
FROM movie_info m
JOIN title t ON m.movie_id = t.id
JOIN cast_info c ON m.movie_id = c.movie_id
JOIN role_type r ON c.role_id = r.id
JOIN movie_companies mc ON m.movie_id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id -- Self join on movie_info to meet the join count requirement
JOIN movie_info m2 ON m.movie_id = m2.movie_id
WHERE m.movie_id >= '132'
  AND m.movie_id <= '173'
  AND t.production_year = '1940'
GROUP BY m.movie_id;