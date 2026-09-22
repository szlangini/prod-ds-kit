SELECT id,
       ROLE
FROM role_type
WHERE id >= CAST('9' AS INTEGER)
  AND id <= CAST('10' AS INTEGER)
  AND
    (SELECT COUNT(*)
     FROM role_type AS sub
     WHERE sub.role = role_type.role) >= CAST('8' AS INTEGER);