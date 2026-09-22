SELECT COUNT(ci.id) AS total_cast,
       AVG(ci.nr_order) AS avg_nr_order,
       MIN(ci.id) AS min_cast_id,
       MAX(ci.id) AS max_cast_id,
       SUM(ci.person_role_id) AS total_person_role,
       COUNT(mi.id) AS total_movie_info_records
FROM cast_info ci
JOIN title t ON ci.movie_id = t.id
JOIN movie_info mi ON t.id = mi.movie_id
WHERE ci.id BETWEEN '14' AND '421'
  AND t.production_year = '1916'
  AND mi.note BETWEEN '{{movie_info.note_lower}}' AND '{{movie_info.note_upper}}';