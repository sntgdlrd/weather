CREATE TABLE condition(
	id INT PRIMARY KEY,
	name VARCHAR(40)
);

INSERT INTO condition (id, name)
SELECT ROW_NUMBER() OVER (ORDER BY name), name
FROM (SELECT DISTINCT condition as name FROM weatherapi) AS subquery;

CREATE TABLE moonphase(
	id INT PRIMARY KEY,
	name VARCHAR(20)
);

INSERT INTO moonphase(id, name)
SELECT ROW_NUMBER() OVER (ORDER BY name), name
FROM (SELECT DISTINCT moon_phase as name FROM weatherapi) AS subquery;

CREATE TABLE city (
	id INT PRIMARY KEY,
	name VARCHAR(40),
	region VARCHAR(40),
	country VARCHAR(40)
);

INSERT INTO city (id, name, region, country)
VALUES 