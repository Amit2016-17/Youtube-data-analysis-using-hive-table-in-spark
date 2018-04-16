-- External table for video

create external  table Stg_video(
video_id string,trending_date string,
title string,channel_title string,category_id int,
publish_time string,tags string,views string,
likes int,dislikes int ,comment_count int,
thumbnail_link string,comments_disabled string,
ratings_disabled string,video_error_or_removed string
)
partitioned by (country string)

ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ","
)
location '/user/cloudera/youtube/input_data/table_data'
tblproperties("skip.header.line.count" = "1");

------ Load the data

Load data inpath '/user/cloudera/youtube/input_data/csv/CAvideos.csv' into table Stg_video
partition(country='CA');
Load data inpath '/user/cloudera/youtube/input_data/csv/DEvideos.csv' into table Stg_video
partition(country='DE');
Load data inpath '/user/cloudera/youtube/input_data/csv/FRvideos.csv' into table Stg_video
partition(country='FR');
Load data inpath '/user/cloudera/youtube/input_data/csv/GBvideos.csv' into table Stg_video
partition(country='GB');
Load data inpath '/user/cloudera/youtube/input_data/csv/USvideos.csv' into table Stg_video
partition(country='US');


-- External table for categoryid

create external table Stg_categoryId
(video_desc string
)
partitioned by (country string)
location '/user/cloudera/youtube/input_data/json_table';

-- Load the data 

load data inpath '/user/cloudera/youtube/input_data/json/DE_catId.json' into table Stg_categoryId
partition (country='DE');
load data inpath '/user/cloudera/youtube/input_data/json/CA_catId.json' into table Stg_categoryId
partition (country='CA');
load data inpath '/user/cloudera/youtube/input_data/json/FR_catId.json' into table Stg_categoryId
partition (country='FR');
load data inpath '/user/cloudera/youtube/input_data/json/GB_catId.json' into table Stg_categoryId
partition (country='GB');
load data inpath '/user/cloudera/youtube/input_data/json/US_catId.json' into table Stg_categoryId
partition (country='US');


