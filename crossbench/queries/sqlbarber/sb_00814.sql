SELECT id,
       keyword
FROM keyword
WHERE id >= '312'
  AND id <= '419'
GROUP BY id,
         keyword;