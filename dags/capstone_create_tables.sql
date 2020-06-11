DROP TABLE IF EXISTS public.dim_city;
CREATE TABLE IF NOT EXISTS public.dim_city (
	city varchar(256), 
	state_code varchar(50), 
	city_id BIGINT,
	PRIMARY KEY(city_id)
);

DROP TABLE IF EXISTS public.dim_airport_codes;
CREATE TABLE IF NOT EXISTS public.dim_airport_codes (
	icao_code varchar(256),
	type varchar(256),
	name varchar(256),
	elevation_ft INT,
	airport_latitude FLOAT8,
	airport_longitude FLOAT8,
	city_id BIGINT,
	PRIMARY KEY(icao_code)
);


DROP TABLE IF EXISTS public.dim_us_cities_demographics;
CREATE TABLE IF NOT EXISTS public.dim_us_cities_demographics (
	median_age FLOAT,
	average_household_size FLOAT,
	race varchar(75),
	male_population BIGINT,
	female_population BIGINT,
	total_population BIGINT,
	num_veterans BIGINT,
	foreign_born BIGINT,
	city_id BIGINT,
	PRIMARY KEY(city_id)
);

DROP TABLE IF EXISTS public.dim_us_cities_temperatures;
CREATE TABLE IF NOT EXISTS public.dim_us_cities_temperatures (
	avg_temp FLOAT8,
	std_temp FLOAT8,
	city_id BIGINT
);

DROP TABLE IF EXISTS public.dim_us_airports_weather;
CREATE TABLE IF NOT EXISTS public.dim_us_airports_weather (
	name varchar(256),
	elevation_ft INT,
	city varchar(256),
	state varchar(50),
	avg_temp FLOAT8,
	std_temp FLOAT8,
	PRIMARY KEY(name)
);

DROP TABLE IF EXISTS public.fact_immigrant_info;
CREATE TABLE IF NOT EXISTS public.fact_immigrant_info (
	cicid INT,
	from_country_code INT2,
	age INT2,
	visa_code INT2,
	visa_post varchar(50),
	occupation varchar(256),
	visa_type varchar(15),
	birth_year INT,
	gender varchar(5),
	arrival_date DATE,
	arrival_month INT,
	arrival_year INT,
	PRIMARY KEY(cicid)
);

DROP TABLE IF EXISTS public.fact_immigration_info;
CREATE TABLE IF NOT EXISTS public.fact_immigration_info (
	cicid INT,
	iata_code varchar(10),
	state_code varchar(50),
	airline varchar(10),
	admnum BIGINT,
	fltno varchar(50),
	arrival_flag varchar(2),
	depature_flag varchar(2),
	update_flag varchar(2),
	arrival_date DATE,
	departure_date DATE,
	allowed_until DATE,
	arrival_month INT,
	arrival_year INT,
	PRIMARY KEY(cicid)
);

DROP TABLE IF EXISTS public.fact_immigration_demographics;
CREATE TABLE IF NOT EXISTS public.fact_immigration_demographics (
	cicid INT,
	age INT2, 
	gender varchar(5),
	occupation varchar(256),
	arrival_month INT,
	arrival_year INT,
	from_country varchar(256),
	state varchar(50),
	median_age FLOAT,
	total_population BIGINT,
	foreign_born BIGINT,
	PRIMARY KEY(cicid)
);



