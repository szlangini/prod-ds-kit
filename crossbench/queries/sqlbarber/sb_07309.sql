SELECT a.*
FROM aka_title a
JOIN role_type r ON a.kind_id = r.id
WHERE a.title <> '{{aka_title.title_exclude}}'
  AND a.id >
    (SELECT AVG(id)
     FROM aka_title
     WHERE id > '259'::integer
       AND id < '411'::integer)
  AND a.production_year = '2005'
  AND r.role = 'production designer';