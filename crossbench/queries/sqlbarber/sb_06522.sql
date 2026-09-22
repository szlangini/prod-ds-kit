SELECT cc.status_id,
       COUNT(*) AS total_count
FROM complete_cast cc
JOIN movie_info mi ON cc.movie_id = mi.movie_id
AND mi.info_type_id = '89'
WHERE cc.id >= '168'
  AND cc.id <= '494'
  AND cc.status_id = '3'
  AND cc.id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '168')
GROUP BY cc.status_id;