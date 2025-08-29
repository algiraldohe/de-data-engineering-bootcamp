-- average number of web events of a session from a user on Tech Creator
SELECT ROUND(AVG(num_hits), 2) avg_web_events
FROM session_processed_events
WHERE host LIKE '%techcreator%';

-- compare results between different hosts
SELECT 
	CASE WHEN host LIKE '%dataexpert%'  THEN 'dataexpert'
		 WHEN host LIKE '%techcreator%'  THEN 'techcreator'
		 WHEN host LIKE '%thecyberinstructor%'  THEN 'thecyberinstructor'
		 WHEN host LIKE '%fullstackexpert%'  THEN 'fullstackexpert'
	ELSE 'N\A' END AS new_host,
	AVG(num_hits) avg_web_events
FROM session_processed_events
GROUP BY 1
ORDER BY avg_web_events DESC;
