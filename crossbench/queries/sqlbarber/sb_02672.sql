SELECT status_id,
       COUNT(*) AS total_count
FROM complete_cast
WHERE id >= '231'
  AND id <= '385'
  AND status_id = '3'
  AND id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '231')
GROUP BY status_id;