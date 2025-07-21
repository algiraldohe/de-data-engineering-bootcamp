-- incremental load for host_activity_datelist
WITH yesterday AS (
	SELECT * FROM hosts_cumulated WHERE date = '2023-01-03'
),

-- today's events processing. cast date and avoid non-identified visitors
today AS (
	SELECT
		host::TEXT AS host,
		event_time::DATE AS date
	FROM events
	WHERE
		user_id IS NOT NULL
		AND event_time::DATE  = '2023-01-04'::DATE
	GROUP BY host,date
)
INSERT INTO hosts_cumulated (
SELECT
	COALESCE(t.host, y.host) AS host,
	-- cumulate dates in array for hosts
	CASE WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date]
		 WHEN t.date IS NULL THEN y.host_activity_datelist
		 WHEN y.host_activity_datelist IS NOT NULL THEN ARRAY[t.date] || y.host_activity_datelist
	END AS host_activity_datelist,
	COALESCE(t.date, y.date) AS date
FROM today t
FULL OUTER JOIN yesterday y ON t.host = y.host

)