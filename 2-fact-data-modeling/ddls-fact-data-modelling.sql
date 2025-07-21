-- create table user_devices_cumulated
-- DROP TABLE user_devices_cumulated;
CREATE TABLE user_devices_cumulated (
	user_id TEXT,
	browser_type TEXT,
	device_activity_datelist DATE[],
	date DATE,
	PRIMARY KEY (user_id, browser_type, date)
);

-- creating hosts cumulative activity table
DROP TABLE hosts_cumulated;
CREATE TABLE hosts_cumulated (
	host TEXT,
	host_activity_datelist DATE[],
	date DATE,
	PRIMARY KEY (host, date)
);


-- creating hosts monthly reduced fact table
-- DROP TABLE host_activity_reduced;
CREATE TABLE host_activity_reduced (
	host TEXT,
	month_start DATE,
	hits_array REAL[],
	unique_visitors_array REAL[],
	PRIMARY KEY(host, month_start)
);