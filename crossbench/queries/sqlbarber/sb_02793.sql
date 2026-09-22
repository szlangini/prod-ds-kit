SELECT status_id,
       COUNT(*) AS total_count
FROM complete_cast
WHERE id >= '158'
  AND id <= '396'
  AND status_id = '4'
  AND id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '158')
GROUP BY status_id;