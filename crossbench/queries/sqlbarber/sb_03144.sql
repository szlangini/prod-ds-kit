SELECT status_id,
       COUNT(*) AS total_count
FROM complete_cast
WHERE id >= '29'
  AND id <= '479'
  AND status_id = '3'
  AND id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '29')
GROUP BY status_id;