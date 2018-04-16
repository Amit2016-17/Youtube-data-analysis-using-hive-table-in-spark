## What are the most viewed 5 videos considering all regions :

```py

top5Video = sqlContext.sql("select title,viewers from\
(select title, viewers,DENSE_RANK() over\
(order by viewers desc) rnk from (\
select title ,sum(views) viewers from video_desc where video_id not like '#NAME?' group by title ) t)t1 where rnk<=5 order by viewers desc ")

top5View.show()


```

## What are the top 5 channel which have maximum no of viewers

```py

top5channel = sqlContext.sql("select channel_title,viewers from\
(select channel_title,viewers,DENSE_RANK() over\
(order by viewers desc) rnk from (\
select channel_title,sum(views) viewers from video_desc group by channel_title) t)t1 where rnk<=5 order by viewers desc ")

```

## Find out the top 3 category for every region which have maximum no video

```py
top3Cat = sqlContext.sql\
("select * from (select country,category_name, cat_count, rank() over (partition by country order by cat_count desc) rank from (select t1.country country,t1.cat_id cat_id,t2.categoryname category_name, count(t1.cat_id) cat_count from\
(select distinct country country,category_id cat_id,video_id from video_desc)t1,categoryid t2 \
where t1.cat_id=t2.id and t1.country=t2.country group by t1.country,t1.cat_id,t2.categoryname)temp)temp1 where rank <=3")

top3Cat.show()
```

##  based on the top 3 category  of every region find out top 2 videos for that top category which have most no likes

```py
 -- have created oe view abc 
sqlContext.sql\
(" create view abc as select * from (select country,category_name, cat_count, rank() over (partition by country order by cat_count desc) rank from (select t1.country country,t1.cat_id cat_id,t2.categoryname category_name, count(t1.cat_id) cat_count from\
(select distinct country country,category_id cat_id,video_id from video_desc)t1,categoryid t2 \
where t1.cat_id=t2.id and t1.country=t2.country group by t1.country,t1.cat_id,t2.categoryname)temp)temp1 where rank <=3")

mostLikeVideos = sqlContext.sql("select * from \
(select t3.country,t3.cat_name,t3.title,t3.noOfLike,rank() over \
(partition by t3.country,t3.cat_name order by t3.noOfLike desc) rank from\
(select t1.country country,t2.categoryname cat_name,t1.title title,sum(t1.likes) noOfLike \
from video_desc t1,categoryid t2 where t1.category_id = t2.id and t1.country=t2.country group by t1.country,t1.title,t2.categoryname)t3,abc t4 where \
t4.category_name=t3.cat_name and t4.country=t3.country)temp5 where rank<=2")

mostLikeVideos.show()





