SELECT mk.movie_id,
       k.keyword,
       COUNT(*) AS occurrence
FROM movie_keyword mk
JOIN keyword k ON mk.keyword_id = k.id
WHERE mk.movie_id >= '1627'
  AND mk.movie_id <= '2910'
GROUP BY mk.movie_id,
         k.keyword;