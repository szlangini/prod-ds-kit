SELECT cc.status_id,
       COUNT(*) AS total_count
FROM complete_cast cc
JOIN movie_link ml ON cc.movie_id = ml.movie_id
WHERE cc.id >= '126'
  AND cc.id <= '251'
  AND cc.status_id = '3'
  AND cc.id >
    (SELECT AVG(cc2.id)
     FROM complete_cast cc2
     WHERE cc2.id >= '126'
       AND cc2.id <= '251')
GROUP BY cc.status_id;