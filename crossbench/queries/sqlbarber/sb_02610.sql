SELECT status_id,
       COUNT(*) AS total_count
FROM complete_cast
WHERE id >= '82'
  AND id <= '179'
  AND status_id = '4'
  AND id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '82')
GROUP BY status_id;