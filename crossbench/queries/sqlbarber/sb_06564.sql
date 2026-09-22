SELECT k.id,
       k.keyword,
       mi.info
FROM keyword AS k
JOIN movie_keyword AS mk ON k.id = mk.keyword_id
JOIN movie_info AS mi ON mk.movie_id = mi.movie_id
WHERE k.id >= '132'
  AND k.id <= '138'
  AND mi.info_type_id = '57'
GROUP BY k.id,
         k.keyword,
         mi.info;