-- Film department employees earning >$100k
select * FROM content_creators
WHERE department_id = 2 AND salary > 100000;
-- department_id = 2

-- Active Premium users in USA OR UK
SELECT DISTINCT u.user_id, u.user_name FROM streaming_users u
INNER JOIN subscriptions s
ON u.user_id = s.user_id
WHERE u.country IN ('USA','UK')
AND s.plan_type = 'Premium'
AND s.status = 'Active';