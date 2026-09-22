SELECT status_id,
       COUNT(*) AS total_count
FROM complete_cast
WHERE id >= '161'
  AND id <= '222'
  AND status_id = '3'
  AND id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '161')
GROUP BY status_id;