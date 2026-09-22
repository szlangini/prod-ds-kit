SELECT cc.status_id,
       COUNT(*) AS total_count
FROM complete_cast cc
JOIN movie_link ml ON cc.movie_id = ml.movie_id
WHERE cc.id >= '237'
  AND cc.id <= '454'
  AND cc.status_id = '3'
  AND cc.id >
    (SELECT AVG(cc2.id)
     FROM complete_cast cc2
     WHERE cc2.id >= '237'
       AND cc2.id <= '454')
GROUP BY cc.status_id;