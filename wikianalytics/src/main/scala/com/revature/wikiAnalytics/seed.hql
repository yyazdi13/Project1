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

-- SELECT * FROM page_views WHERE page NOT LIKE ('%Main%', '%Special%') AND language = 'en' ORDER BY views DESC LIMIT 10;

CREATE TABLE ENGLISH_PAGES (
page STRING,
views INT,
bytes INT
) PARTITIONED BY (language STRING)
ROW FORMAT DELIMited
FIELDS TERMINATED BY ',';

INSERT INTO TABLE english_pages PARTITION(language='en')
SELECT page, views, bytes FROM page_views WHERE page NOT LIKE '%Main%' AND language='en';

-- SELECT * FROM english_pages WHERE page NOT LIKE '%Special%' ORDER BY views DESC LIMIT 50;

SELECT page, SUM(views) views
FROM english_pages
GROUP BY page
ORDER BY views DESC;

