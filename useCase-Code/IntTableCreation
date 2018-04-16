create  table video_desc(
video_id string,trending_date string,
title string,channel_title string,category_id int,
publish_time string,views string,
likes int,dislikes int ,comment_count int
)
partitioned by (country string)
STORED AS PARQUET;

insert into table video_desc partition (country='FR')
select video_id,trending_date,title,channel_title,category_id,publish_time,views,likes,dislikes,comment_count
from Stg_video where country='FR';

insert into table video_desc partition (country='CA')
select video_id,trending_date,title,channel_title,category_id,publish_time,views,likes,dislikes,comment_count
from Stg_video where country='CA';

insert into table video_desc partition (country='DE')
select video_id,trending_date,title,channel_title,category_id,publish_time,views,likes,dislikes,comment_count
from Stg_video where country='DE';

insert into table video_desc partition (country='GB')
select video_id,trending_date,title,channel_title,category_id,publish_time,views,likes,dislikes,comment_count
from Stg_video where country='GB';

insert into table video_desc partition (country='US')
select video_id,trending_date,title,channel_title,category_id,publish_time,views,likes,dislikes,comment_count
from Stg_video where country='US';

------------------------------
create table categoryID(
id string,
categoryName string
)
partitioned by(country string)
stored as PARQUET;

insert into table categoryID partition(country ='CA')
select GET_JSON_OBJECT(video_desc,'$.id') ,GET_JSON_OBJECT(video_desc,'$.snippet.title')
FROM Stg_categoryId where country = 'CA';
insert into table categoryID partition(country ='FR')
select GET_JSON_OBJECT(video_desc,'$.id') ,GET_JSON_OBJECT(video_desc,'$.snippet.title')
FROM Stg_categoryId where country = 'FR';
insert into table categoryID partition(country ='GB')
select GET_JSON_OBJECT(video_desc,'$.id') ,GET_JSON_OBJECT(video_desc,'$.snippet.title')
FROM Stg_categoryId where country = 'GB';
insert into table categoryID partition(country ='DE')
select GET_JSON_OBJECT(video_desc,'$.id') ,GET_JSON_OBJECT(video_desc,'$.snippet.title')
FROM Stg_categoryId where country = 'DE';
insert into table categoryID partition(country ='CA')
select GET_JSON_OBJECT(video_desc,'$.id') ,GET_JSON_OBJECT(video_desc,'$.snippet.title')
FROM Stg_categoryId where country = 'DE';
