SELECT MAX(ci.nr_order) AS max_nr_order,
       MIN(mi.info) AS min_info_value,
       COUNT(DISTINCT t.title) AS distinct_titles,
       SUM(mc.company_id) AS sum_company_id,
       AVG(pi.person_id) AS avg_person_id
FROM cast_info AS ci
JOIN movie_info AS mi ON ci.movie_id = mi.movie_id
JOIN title AS t ON mi.movie_id = t.id
JOIN person_info AS pi ON ci.person_id = pi.person_id
JOIN movie_keyword AS mk ON mi.movie_id = mk.movie_id
JOIN movie_companies AS mc ON mi.movie_id = mc.movie_id
JOIN company_name AS cn ON mc.company_id = cn.id
JOIN cast_info AS ci2 ON ci.movie_id = ci2.movie_id
WHERE mi.movie_id = '272'
  AND ci.role_id = '11'
  AND pi.person_id BETWEEN '2102' AND '3492';