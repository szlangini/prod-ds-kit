SELECT n.id,
       n.name,
       n.imdb_id,
       COUNT(ci.id) AS movie_count
FROM name n
JOIN cast_info ci ON n.id = ci.person_id
WHERE n.id BETWEEN '477' AND '490'
  AND n.gender = 'm'
  AND ci.role_id = '9'
GROUP BY n.id,
         n.name,
         n.imdb_id;