-- incremental query to populate user_devices_cumulated
WITH yesterday AS (
	SELECT * FROM user_devices_cumulated
	WHERE date = DATE('2023-01-30')
),

-- process today's events
today AS (
	SELECT
		e.user_id::TEXT AS user_id,
		d.browser_type,
		DATE(e.event_time::TIMESTAMP) AS  date_active

	FROM events e
	INNER JOIN devices d ON  e.device_id = d.device_id
	WHERE
		e.user_id  IS NOT NULL
		AND d.device_id IS NOT NULL
		AND DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
	GROUP BY 1,2,3
)

INSERT INTO user_devices_cumulated (
SELECT
	COALESCE(t.user_id, y.user_id) AS  user_id,
	COALESCE(t.browser_type, y.browser_type) AS browser_type,
	-- compute datelist array that cumulates the activity dates (day by day)
	CASE WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
		 WHEN t.date_active IS NULL THEN y.device_activity_datelist
		 WHEN y.device_activity_datelist IS NOT NULL THEN ARRAY[t.date_active] || y.device_activity_datelist
		END AS dates_active,
	COALESCE(t.date_active, y.date + INTERVAL '1 day') AS  date

FROM today t
FULL OUTER JOIN yesterday y ON y.user_id = t.user_id AND y.browser_type = t.browser_type
);