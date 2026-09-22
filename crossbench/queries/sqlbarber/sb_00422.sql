SELECT movie_companies.note
FROM movie_companies
WHERE movie_companies.id BETWEEN '130' AND '171'
  AND movie_companies.note = '(1996-2003) (USA) (TV)'
GROUP BY movie_companies.note;