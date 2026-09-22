SELECT cc.status_id,
       COUNT(*) AS total_count
FROM complete_cast cc
JOIN movie_info mi ON cc.movie_id = mi.movie_id
AND mi.info_type_id = '61'
WHERE cc.id >= '129'
  AND cc.id <= '130'
  AND cc.status_id = '4'
  AND cc.id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '129')
GROUP BY cc.status_id;