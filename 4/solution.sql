WITH cummulative_table AS (
	SELECT 
		phrase, 
		toDate(dt) as date,
		toHour(dt) AS hour,
		MAX(views) AS "views"
	FROM phrases_views
	WHERE 
		campaign_id = 1111111
		AND date = today()
	GROUP BY phrase, date, hour
	
	UNION ALL
	
	SELECT 
		phrase, 
		today() - INTERVAL 1 day,
		0,
		MAX(views) AS "views"
	FROM phrases_views
	WHERE 
		campaign_id = 1111111
		AND dt < today()
	GROUP BY phrase
)


SELECT phrase, groupArray("views_in_hour") AS "views_by_hour"
FROM (
	SELECT 
		phrase, 
		date,
		(hour,  views - lag(views, 1, 0) OVER (PARTITION BY phrase ORDER BY date, hour)) AS "views_in_hour"
	FROM cummulative_table 
	ORDER BY phrase, hour DESC
)
WHERE date = today()
GROUP BY phrase
