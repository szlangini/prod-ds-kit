SELECT cc.status_id,
       COUNT(*) AS total_count
FROM complete_cast cc
JOIN movie_info mi ON cc.movie_id = mi.movie_id
AND mi.info_type_id = '41'
WHERE cc.id >= '27'
  AND cc.id <= '251'
  AND cc.status_id = '4'
  AND cc.id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '27')
GROUP BY cc.status_id;