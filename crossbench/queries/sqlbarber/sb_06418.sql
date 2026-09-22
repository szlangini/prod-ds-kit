SELECT n.id,
       n.name,
       n.imdb_id,
       COUNT(ci.id) AS movie_count
FROM name n
JOIN cast_info ci ON n.id = ci.person_id
WHERE n.id BETWEEN '32' AND '372'
  AND n.gender = 'f'
  AND ci.role_id = '7'
GROUP BY n.id,
         n.name,
         n.imdb_id;