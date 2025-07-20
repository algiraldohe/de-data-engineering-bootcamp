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
CREATE TABLE hosts_cumulated (
	host TEXT,
	host_activity_datelist DATE[],
	date DATE,
	PRIMARY KEY (host, date)
);