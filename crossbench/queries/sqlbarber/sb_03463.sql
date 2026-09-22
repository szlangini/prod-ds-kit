SELECT n.id,
       n.name,
       n.imdb_id,
       COUNT(ci.id) AS movie_count
FROM name n
JOIN cast_info ci ON n.id = ci.person_id
WHERE n.id BETWEEN '443' AND '455'
  AND n.gender = 'f'
  AND ci.role_id = '2'
GROUP BY n.id,
         n.name,
         n.imdb_id;