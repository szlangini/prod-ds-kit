SELECT status_id,
       COUNT(*) AS total_count
FROM complete_cast
WHERE id >= '206'
  AND id <= '321'
  AND status_id = '4'
  AND id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '206')
GROUP BY status_id;