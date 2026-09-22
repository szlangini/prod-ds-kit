SELECT status_id,
       COUNT(*) AS total_count
FROM complete_cast
WHERE id >= '45'
  AND id <= '444'
  AND status_id = '3'
  AND id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '45')
GROUP BY status_id;