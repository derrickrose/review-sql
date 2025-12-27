-- How many records are there
SELECT count(*) as total from content_creators
-- includes NULL
-- count(column) exclude NULL from that column
-- count(DISTINCT column)