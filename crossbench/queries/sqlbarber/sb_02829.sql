SELECT status_id,
       COUNT(*) AS total_count
FROM complete_cast
WHERE id >= '100'
  AND id <= '357'
  AND status_id = '3'
  AND id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '100')
GROUP BY status_id;