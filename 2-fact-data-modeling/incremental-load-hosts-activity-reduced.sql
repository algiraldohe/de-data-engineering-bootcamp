-- incremental load for metrics (hits, visitors)
WITH daily_aggregate AS (
	SELECT
		host,
		event_time::DATE AS date,
		COUNT(1) AS number_site_hits,
		COUNT(DISTINCT user_id) AS number_unique_visitors
	FROM events
	WHERE user_id IS NOT NULL AND event_time::DATE = '2023-01-05'::DATE
	GROUP BY host, date
),

yesterday_array AS (
	SELECT * FROM host_activity_reduced
	WHERE month_start = '2023-01-01'::DATE
)

INSERT INTO host_activity_reduced (
SELECT
	COALESCE(da.host, ya.host) AS host,
	COALESCE(ya.month_start, DATE_TRUNC('month', da.date))::DATE AS month_start,
	-- compute array metrics for hits
	CASE
		WHEN ya.hits_array IS NOT NULL THEN ya.hits_array || ARRAY[COALESCE(da.number_site_hits, 0)]
		WHEN ya.month_start IS NULL THEN ARRAY[COALESCE(da.number_site_hits, 0)]
		WHEN ya.hits_array IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(date - month_start, 0)]) || ARRAY[COALESCE(da.number_site_hits, 0)]
	END AS hits_array,
    -- compute array metric for unique_visitors
	CASE
		WHEN ya.unique_visitors_array IS NOT NULL THEN ya.unique_visitors_array || ARRAY[COALESCE(da.number_unique_visitors, 0)]
		WHEN ya.month_start IS NULL THEN ARRAY[COALESCE(da.number_unique_visitors, 0)]
		WHEN ya.unique_visitors_array IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(date - month_start, 0)]) || ARRAY[COALESCE(da.number_unique_visitors, 0)]
	END AS unique_visitors_array

FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya ON da.host = ya.host
)
-- update if the records already exist with the new information for the aray metrics (hits, unique_visitors)
ON CONFLICT(host, month_start) DO UPDATE SET hits_array = EXCLUDED.hits_array, unique_visitors_array = EXCLUDED.unique_visitors_array;

-- aggregate metrics by summing specific elements in the array metrics
WITH aggregated AS (
	SELECT
		month_start,
		ARRAY[
			SUM(hits_array[1]),
			SUM(hits_array[2]),
			SUM(hits_array[3])
		] AS site_hits,

		ARRAY[
			SUM(unique_visitors_array[1]),
			SUM(unique_visitors_array[2]),
			SUM(unique_visitors_array[3])
		] AS total_unique_visitors

	FROM host_activity_reduced
	GROUP BY month_start
)
-- select and display the date (adjusted by index), and summed value
SELECT
    month_start + CAST(CAST(index - 1 AS TEXT) || ' day' AS INTERVAL) AS adjusted_date,
    elem AS value
FROM aggregated agg
CROSS JOIN UNNEST(agg.site_hits) WITH ORDINALITY AS a(elem, index);
