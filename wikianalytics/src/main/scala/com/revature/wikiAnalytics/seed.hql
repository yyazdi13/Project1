--QUESTION 1:
CREATE DATABASE wiki;
USE wiki;

CREATE TABLE page_views (
language STRING,
page STRING,
views INT,
bytes INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ';

LOAD DATA LOCAL INPATH '/home/yyazdi/wikipageviews' INTO TABLE page_views;


CREATE TABLE ENGLISH_PAGES (
page STRING,
views INT,
bytes INT
) PARTITIONED BY (language STRING)
ROW FORMAT DELIMited
FIELDS TERMINATED BY ',';

INSERT INTO TABLE english_pages PARTITION(language='en')
SELECT page, views, bytes FROM page_views WHERE page NOT LIKE '%Main%' AND language='en';

SELECT page, SUM(views) views
FROM english_pages
GROUP BY page
ORDER BY views DESC;

--QUESTION 2:
CREATE TABLE clickstreams (
prev STRING,
curr STRING,
type STRING,
occurrences INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

--this file is actually from my map reduce output
LOAD DATA LOCAL INPATH '/home/yyazdi/clickstream.tsv' INTO TABLE clickstreams;

SELECT * FROM english_pages;
SELECT * FROM clickstreams; 
ORDER BY streams DESC;

SELECT c.streams, c.prev, (e.views * 24 * 31) AS views, ROUND(c.streams/views, 2) AS fraction
FROM clickstreams c JOIN english_pages e
ON (c.curr = e.page)
ORDER BY fraction DESC
LIMIT 50;

--QUESTION 4:
CREATE TABLE not_america (
language STRING,
page STRING,
views INT,
bytes INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ';

LOAD DATA LOCAL INPATH '/home/yyazdi/wikipageviews/pageviews-20210120-050000' INTO TABLE not_america;

CREATE TABLE notAmericaEn (
page STRING,
views INT,
bytes INT
) PARTITIONED BY (language STRING)
ROW FORMAT DELIMited
FIELDS TERMINATED BY ',';

INSERT INTO TABLE notAmericaEn PARTITION(language='en')
SELECT page, views, bytes FROM not_america WHERE page NOT LIKE '%Main%' AND language='en';

CREATE TABLE america (
language STRING,
page STRING,
views INT,
bytes INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ';

LOAD DATA LOCAL INPATH '/home/yyazdi/wikipageviews/pageviews-20210120-110000' INTO TABLE america;

CREATE TABLE americaEn (
page STRING,
views INT,
bytes INT
) PARTITIONED BY (language STRING)
ROW FORMAT DELIMited
FIELDS TERMINATED BY ',';

INSERT INTO TABLE americaEn PARTITION(language='en')
SELECT page, views, bytes FROM america WHERE page NOT LIKE '%Main%' AND language='en';

SELECT a.page, a.views AS AmericaViews, n.views AS notAmericaViews
FROM americaen a 
INNER JOIN notamericaen n 
ON (a.page = n.page)
ORDER BY AmericaViews DESC;

--QUESTION 3:
--Hotel_California -> Hotel_California_(Eagles_album) -> The_Long_Run_(album)

CREATE TABLE click_series (
curr STRING,
type STRING,
occurrences INT
) PARTITIONED BY (prev STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT INTO TABLE click_series PARTITION(prev='Hotel_California')
SELECT curr, type, occurrences FROM clickstreams WHERE prev='Hotel_California';

SELECT * FROM clickstreams WHERE prev = 'Hotel_California_(Eagles_album)' ORDER BY occurrences DESC;

SELECT SUM(views) FROM english_pages WHERE page = 'Eagles_(band)';

SELECT c.occurrences, c.curr, ((e.views * 24) * 31) AS views, (c.occurrences / ((views * 24 )* 31)) AS fraction
FROM eagles_album c LEFT JOIN english_pages e
ON (c.curr = e.page)
ORDER BY fraction DESC;

CREATE TABLE eagles_album(
curr STRING,
type STRING,
occurrences INT
) PARTITIONED BY (prev STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT INTO TABLE eagles_album PARTITION(prev='Hotel_California_(Eagles_album)')
SELECT curr, type, occurrences FROM clickstreams WHERE prev='Hotel_California_(Eagles_album)';

SELECT * FROM eagles_album ORDER BY occurrences DESC;

--Question 5