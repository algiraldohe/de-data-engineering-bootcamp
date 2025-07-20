-- converting device_activity_datelist into a datelist_int for performance
WITH users AS (
	SELECT * FROM public.user_devices_cumulated
	WHERE date = '2023-01-31'
),
-- generating the date series to compare active dates against
series AS (
	SELECT *
	FROM generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 day') AS  series_date
),
-- converting true/false active date to a power of 2
place_holder_ints AS (
SELECT
	(CASE WHEN
			device_activity_datelist @> ARRAY[series_date::DATE]
			THEN POW(2, 32 - (date - series_date::DATE)) -- exponent gets bigger the more recent the activity was
	ELSE 0 END) AS placeholder_int_value,
	date - series_date::DATE AS days_since,
	*
FROM users u
CROSS JOIN series d)

-- counting bits as days active on a 32-bit memory chain.
-- the output gives days active on a 30 days basis by user_id and browser_type by cut-off date.
SELECT
	user_id,
	browser_type,
	BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) AS device_activity_datelist_int,
	date
FROM place_holder_ints
GROUP BY user_id, browser_type, date;

