SELECT mk.movie_id,
       k.keyword,
       COUNT(*) AS occurrence
FROM movie_keyword mk
JOIN keyword k ON mk.keyword_id = k.id
WHERE mk.movie_id >= '591'
  AND mk.movie_id <= '2972'
GROUP BY mk.movie_id,
         k.keyword;