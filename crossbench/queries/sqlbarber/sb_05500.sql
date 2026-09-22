SELECT cc.status_id,
       COUNT(*) AS total_count
FROM complete_cast cc
JOIN movie_info mi ON cc.movie_id = mi.movie_id
AND mi.info_type_id = '63'
WHERE cc.id >= '62'
  AND cc.id <= '153'
  AND cc.status_id = '3'
  AND cc.id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '62')
GROUP BY cc.status_id;