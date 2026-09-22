SELECT n.id,
       n.name,
       n.imdb_id,
       COUNT(ci.id) AS movie_count
FROM name n
JOIN cast_info ci ON n.id = ci.person_id
WHERE n.id BETWEEN '82' AND '92'
  AND n.gender = 'm'
  AND ci.role_id = '3'
GROUP BY n.id,
         n.name,
         n.imdb_id;