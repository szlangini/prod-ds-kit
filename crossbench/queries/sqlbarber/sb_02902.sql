SELECT status_id,
       COUNT(*) AS total_count
FROM complete_cast
WHERE id >= '28'
  AND id <= '342'
  AND status_id = '4'
  AND id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '28')
GROUP BY status_id;