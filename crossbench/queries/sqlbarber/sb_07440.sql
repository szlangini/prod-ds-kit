SELECT cc.status_id,
       COUNT(*) AS total_count
FROM complete_cast cc
JOIN movie_info mi ON cc.movie_id = mi.movie_id
WHERE cc.id >= '116'
  AND cc.id <= '220'
  AND cc.status_id = '3'
  AND mi.info_type_id = '73'
  AND cc.id >
    (SELECT AVG(cc2.id)
     FROM complete_cast cc2
     JOIN movie_info mi2 ON cc2.movie_id = mi2.movie_id
     WHERE cc2.id >= '116'
       AND mi2.info_type_id = '73')
GROUP BY cc.status_id;