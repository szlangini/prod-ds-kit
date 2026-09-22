SELECT n.id,
       n.name,
       n.imdb_id,
       COUNT(c.id) AS cast_count
FROM name n
JOIN cast_info c ON n.id = c.person_id
WHERE n.id BETWEEN '97' AND '129'
  AND n.gender = 'f'
  AND c.role_id = '4'
GROUP BY n.id,
         n.name,
         n.imdb_id;