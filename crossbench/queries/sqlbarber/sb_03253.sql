SELECT status_id,
       COUNT(*) AS total_count
FROM complete_cast
WHERE id >= '4'
  AND id <= '499'
  AND status_id = '3'
  AND id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '4')
GROUP BY status_id;