-- incremental load for host_activity_datelist

WITH yesterday AS (
	SELECT * FROM hosts_cumulated WHERE date = '2023-01-03'
),

datetime_preprocessed AS (
	SELECT
		user_id,
		host,
		event_time::DATE AS date
	FROM events
	WHERE user_id IS NOT NULL
),

today AS (

	SELECT
		host::TEXT AS host,
		COUNT(1) AS hits,
		COUNT(DISTINCT user_id) AS unique_visitors,
		date
	FROM datetime_preprocessed
	WHERE
		date = '2023-01-04'
	GROUP BY host,date
)
INSERT INTO hosts_cumulated (
SELECT
	COALESCE(t.host, y.host) AS host,
	CASE WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date]
		 WHEN t.date IS NULL THEN y.host_activity_datelist
		 WHEN t.date IS NOT NULL THEN ARRAY[t.date] || y.host_activity_datelist
	END AS host_activity_datelist,

	CASE WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.hits]
		 WHEN t.date IS NULL THEN y.hit_array
		 WHEN t.date IS NOT NULL THEN ARRAY[t.hits] || y.hit_array
	END AS hit_array,

	CASE WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.unique_visitors]
		 WHEN t.date IS NULL THEN y.unique_visitors_array
		 WHEN t.date IS NOT NULL THEN ARRAY[t.unique_visitors] || y.unique_visitors_array
	END AS unique_visitors_array,

	COALESCE(t.date, y.date) AS date
FROM today t
FULL OUTER JOIN yesterday y ON t.host = y.host

)

SELECT * FROM hosts_cumulated;