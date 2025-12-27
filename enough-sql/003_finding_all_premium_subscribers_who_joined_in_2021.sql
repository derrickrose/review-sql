SELECT DISTINCT u.user_id  , u.user_name, u.signup_date
FROM streaming_users u
INNER JOIN subscriptions s
ON u.user_id = s.user_id
WHERE s.plan_type = 'Premium'
AND extract(YEAR FROM signup_date) = '2021'

