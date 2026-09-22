SELECT mk.movie_id,
       k.keyword,
       COUNT(*) AS occurrence
FROM movie_keyword mk
JOIN keyword k ON mk.keyword_id = k.id
WHERE mk.movie_id >= '654'
  AND mk.movie_id <= '3556'
GROUP BY mk.movie_id,
         k.keyword;