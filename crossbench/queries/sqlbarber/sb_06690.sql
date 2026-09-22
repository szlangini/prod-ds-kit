SELECT a.*
FROM aka_title a
JOIN role_type r ON a.kind_id = r.id
WHERE a.title <> '{{aka_title.title_exclude}}'
  AND a.id >
    (SELECT AVG(id)
     FROM aka_title
     WHERE id > '5'::integer
       AND id < '5'::integer)
  AND a.production_year = '1890'
  AND r.role = 'cinematographer';