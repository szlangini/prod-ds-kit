SELECT mk.movie_id,
       k.keyword,
       COUNT(*) AS occurrence
FROM movie_keyword mk
JOIN keyword k ON mk.keyword_id = k.id
WHERE mk.movie_id >= '1063'
  AND mk.movie_id <= '4152'
GROUP BY mk.movie_id,
         k.keyword;