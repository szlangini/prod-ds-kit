SELECT COUNT(cast_info.id) AS total_cast,
       AVG(cast_info.nr_order) AS avg_nr_order,
       MIN(cast_info.id) AS min_cast_id,
       MAX(cast_info.id) AS max_cast_id,
       SUM(cast_info.person_role_id) AS total_person_role,
       COUNT(name.id) AS total_name_records
FROM cast_info
JOIN cast_info AS name ON cast_info.person_id = name.person_id
JOIN cast_info AS person_info ON cast_info.person_id = person_info.person_id
WHERE cast_info.id BETWEEN '303' AND '311'
  AND cast_info.person_role_id = '469'
  AND person_info.note = ':fansite';