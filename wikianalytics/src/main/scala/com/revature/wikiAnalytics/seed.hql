--QUESTION 1:
CREATE DATABASE wiki;
USE wiki;

--creating and loading page view data (about 10 files) from Jan 20th
CREATE TABLE page_views (
language STRING,
page STRING,
views INT,
bytes INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ';

LOAD DATA LOCAL INPATH '/home/yyazdi/wikipageviews' INTO TABLE page_views;

--creating a partition by language (en)
CREATE TABLE ENGLISH_PAGES (
page STRING,
views INT,
bytes INT
) PARTITIONED BY (language STRING)
ROW FORMAT DELIMited
FIELDS TERMINATED BY ',';

INSERT INTO TABLE english_pages PARTITION(language='en')
SELECT page, views, bytes FROM page_views WHERE page NOT LIKE '%Main%' AND language='en';

--sum views by page and order them by views descending to see the most poplular page on Jan 20th
SELECT page, SUM(views) views
FROM english_pages
GROUP BY page
ORDER BY views DESC
LIMIT 10;

--Here's the results/top hits:
--Joe_Biden       82560 * 14 = 1,155,840 (that should be a closer estimate since I only used 10 hrs)
--Bible   52661
--Kamala_Harris   49552
--Donald_Trump    48524
--List_of_people_granted_executive_clemency_by_Donald_Trump       38965
--Dan_Quayle      35374

--QUESTION 2:
--make and load clickstream table
CREATE TABLE clickstreams (
prev STRING,
curr STRING,
type STRING,
occurrences INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/yyazdi/clickstream.tsv' INTO TABLE clickstreams;


--I reloaded my pageviews table with only one hour of data and repartitioned it by language into english pages table
--Since pageviews is hourly and we want to compare clickstreams which is monthly, 
--I multiplied the views by 24 hours, then multiplied that by 31 to get closer to a month's worth of views
--To see the percentage of clicks to views, I divided the number of clicks by the total views for each page,
--Then I joined pageviews and clickstream data and ordered it by the fraction I just made (descending)
SELECT c.streams, c.prev, (e.views * 24 * 31) AS views, ROUND(c.streams/views, 2) AS fraction
FROM clickstreams c JOIN english_pages e
ON (c.curr = e.page)
ORDER BY fraction DESC
LIMIT 50;

--results:
--Princess_Margaret_Countess_of_Snowdon
--Diana, Princess of Wales
--Prince Phillip
--Tenet
--Soul
--It's a wonderful life
--Queen Victoria

--QUESTION 4:
--I'm creating two tables, and loading one with pagewviews from early/late night hours,
--and the other with normal hours (CST)
CREATE TABLE not_america (
language STRING,
page STRING,
views INT,
bytes INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ';

LOAD DATA LOCAL INPATH '/home/yyazdi/wikipageviews/pageviews-20210120-050000' INTO TABLE not_america;

--Need to partition by language (en) as well:
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

--Now we can join the two tables and compare their views based on the difference
SELECT a.page, a.views AS AmericaViews, n.views AS notAmericaViews, (a.views - n.views) AS difference
FROM americaen a 
INNER JOIN notamericaen n 
ON (a.page = n.page)
ORDER BY difference DESC
LIMIT 10;

--results:
--Dan_Quayle 30,190(difference)

--QUESTION 3:
--I'll create a table from clickstreams and partition it where the previous page = 'Hotel_California'
CREATE TABLE click_series (
curr STRING,
type STRING,
occurrences INT
) PARTITIONED BY (prev STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT INTO TABLE click_series PARTITION(prev='Hotel_California')
SELECT curr, type, occurrences FROM clickstreams WHERE prev='Hotel_California';

--Now we see which page has the most clicks to pageview ratio
--again we multiply the views * 24 * 31 to represent a month so we can compare to monthly clickstreams
--this gets me Hotel_California_(Eagles_album) as the top hit (~.5)
SELECT c.occurrences, c.curr, ((e.views * 24) * 31) AS views, (c.occurrences / ((views * 24 )* 31)) AS fraction
FROM click_series c LEFT JOIN english_pages e
ON (c.curr = e.page)
ORDER BY fraction DESC;

--So now we create a new table and load clickstreams again, but this time we partition on where
--the previous page = Hotel_California_(Eagles_album)
CREATE TABLE eagles_album(
curr STRING,
type STRING,
occurrences INT
) PARTITIONED BY (prev STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT INTO TABLE eagles_album PARTITION(prev='Hotel_California_(Eagles_album)')
SELECT curr, type, occurrences FROM clickstreams WHERE prev='Hotel_California_(Eagles_album)';

--And now we see which page has the most clicks to pageview ratio from the eagles album page
--This gets me The_Long_Run_(album) (~.6)
SELECT c.occurrences, c.curr, ((e.views * 24) * 31) AS views, (c.occurrences / ((views * 24 )* 31)) AS fraction
FROM eagles_album c LEFT JOIN english_pages e
ON (c.curr = e.page)
ORDER BY fraction DESC;

--Results: 
--Hotel_California -> Hotel_California_(Eagles_album) -> The_Long_Run_(album)

--Question 5
--Need to create the wiki dumps table, I just copied the fields that wikipedia had on their page:
CREATE TABLE wiki_dumps (
wiki_db	string,
event_entity string,
event_type string,
event_timestamp date,
event_comment string,
event_user_id bigint,
event_user_text_historical string,
event_user_text	string,
event_user_blocks_historical array<string>,
event_user_blocks array<string>,
event_user_groups_historical array<string>,
event_user_groups array<string>,
event_user_is_bot_by_historical	array<string>,
event_user_is_bot_by array<string>,
event_user_is_created_by_self	boolean,
event_user_is_created_by_system	boolean,
event_user_is_created_by_peer	boolean,
event_user_is_anonymous	boolean,
event_user_registration_timestamp	string,
event_user_creation_timestamp	string,
event_user_first_edit_timestamp	string,
event_user_revision_count	bigint,
event_user_seconds_since_previous_revision	bigint,
page_id	bigint,
page_title_historical	string,
page_title	string,
page_namespace_historical	int,
page_namespace_is_content_historical	boolean,
page_namespace	int,
page_namespace_is_content	boolean,
page_is_redirect	boolean,
page_is_deleted	boolean,
page_creation_timestamp	string,
page_first_edit_timestamp	string,
page_revision_count	bigint,
page_seconds_since_previous_revision	bigint,
user_id	bigint,
user_text_historical	string,
user_text	string,
user_blocks_historical	array<string>,
user_blocks	array<string>,
user_groups_historical	array<string>,
user_groups	array<string>,
user_is_bot_by_historical	array<string>,
user_is_bot_by	array<string>,
user_is_created_by_self	boolean,
user_is_created_by_system	boolean,
user_is_created_by_peer	boolean,	
user_is_anonymous	boolean,
user_registration_timestamp	string,	
user_creation_timestamp	string,	
user_first_edit_timestamp	string,	
revision_id	bigint,
revision_parent_id	bigint,
revision_minor_edit	boolean,	
revision_deleted_parts	array<string>,
revision_deleted_parts_are_suppressed	boolean,	
revision_text_bytes	bigint,
revision_text_bytes_diff bigint,
revision_text_sha1	string,
revision_content_model	string,
revision_content_format	string,	
revision_is_deleted_by_page_deletion boolean,	
revision_deleted_by_page_deletion_timestamp	string,
revision_is_identity_reverted	boolean,	
revision_first_identity_reverting_revision_id	bigint,	
revision_seconds_to_identity_revert	bigint,
revision_is_identity_revert	boolean,
revision_is_from_before_page_creation	boolean,
revision_tags	array<string>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/yyazdi/wikidumps3.tsv' INTO TABLE wiki_dumps;
DROP TABLE wiki_dumps;

SELECT COUNT(*) event_comment
FROM wiki_dumps WHERE event_comment LIKE '%Vandal%';

SELECT COUNT(*) event_entity FROM wiki_dumps;
--I divided the two numbers above and got ~.001
--this represents the ratio of vandalized pages compared to all pages

SELECT SUM(views) FROM english_pages;
--I then divided this number above by 10 (because I had about 10 hours of data)
--then I divided that by 60 to get the minute and divided by 60 again to get seconds
--then I multiplied that by .001 (the average number of vandalized pages)

--results:
-- ~2 views per second

--QUESTION 6:

--I did use MapReduce in this but deleted my code, thinking I wasn't going to use it
--(it was mostly in the Reduce.scala file). You might still be able to see it in one of the old commits
--but I have the output saved to a file. It filters clickstreams for the previous page starting with H,
--as long as it's more than 3 characters. Starting with HOTE. I put every letter in an array and matched characters to
--the index and if it was less than the letter I wanted, it gets filtered out. E.g if (array.indexOf(char) < 8)

--I make my table of clickstreams that start with HOTE (or at least it was something close to that)
CREATE TABLE clickstreams_starting_with_h (
prev STRING,
curr STRING,
streams INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/yyazdi/ckoutput4/hotelcali' INTO TABLE clickstreams_starting_with_h;

--And this is pretty much what I did for question 2
SELECT c.streams, c.prev, e.views * 24 * 31 AS views, ROUND(c.streams / (e.views * 24 * 31), 2) AS fraction
FROM clickstreams_starting_with_h c JOIN english_pages e
ON (c.prev = e.page)
ORDER BY fraction DESC
LIMIT 50;

