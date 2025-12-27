-- "All creators named John/James/Jennifer

SELECT * from content_creators
WHERE first_name LIKE 'J%';

-- Exactly 4 characters


SELECT * from content_creators
WHERE first_name like 'J_____%'
