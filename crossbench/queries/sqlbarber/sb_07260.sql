SELECT a.*
FROM aka_title a
JOIN role_type r ON a.kind_id = r.id
WHERE a.title <> '{{aka_title.title_exclude}}'
  AND a.id >
    (SELECT AVG(id)
     FROM aka_title
     WHERE id > '227'::integer
       AND id < '519'::integer)
  AND a.production_year = '2014'
  AND r.role = 'editor';