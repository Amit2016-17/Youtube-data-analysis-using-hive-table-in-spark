# Youtub data analysis using hive table in spark

YouTube (the world-famous video sharing website) maintains a list of the top trending videos on the platform. According to Variety magazine,  YouTube uses a combination of factors including measuring users interactions (number of views, shares, comments and likes).
    
*  ## [Dataset](https://github.com/rakeshdey0018/Youtube-data-analysis-using-hive-table-in-spark/upload/master/Dataset)
  
  This dataset includes several months (and counting) of data on daily trending YouTube videos. Data is included for the US, GB, DE, CA, and FR regions `(USA, Great Britain, Germany, Canada, and France, respectively)`, with up to 200 listed trending videos per day.
Each region’s data is in a separate file. Data includes the video title, channel title, publish time, tags, views, likes and dislikes, and comment count.
The data also includes a category_id field, which varies between regions. To retrieve the categories for a specific video, find it in the associated `JSON`. One such file is included for each of the five regions in the dataset.

   Total records of 5 main datasets are `1,24,671`

In each dataset of respective country there may be  multiple records for  particular video_id because one video was trended for multiple days and different likes,comments for different days. 
Also one particular video_id can exist in multiple dataset of different countries. 

*  ## [Use Cases and code]()

  [External Table creation and Raw data load](https://github.com/rakeshdey0018/Youtube-data-analysis-using-hive-table-in-spark/blob/master/useCase-Code/ExtTableCreationDataLoad.hql)
 
  First staging external  table(stg_video) in hive has been created using HUE console to load the raw data.  static partition by country for that table has been used for better performance . Same way External  table (stg_categoryid) has been created  to load the json data of category id. 
  
 [Internal table creation and data load from staging table](https://github.com/rakeshdey0018/Youtube-data-analysis-using-hive-table-in-spark/blob/master/useCase-Code/IntTableCreation.hql)

Then two internal table video_desc and categoryid has been created and stored it as PARQUET file. Then data with required columns  have been loaded from staging table stg_video and stg_categoryid respectively.

  [use cases](https://github.com/rakeshdey0018/Youtube-data-analysis-using-hive-table-in-spark/blob/master/useCase-Code/useCase.md)

All the hive tables have been used in spark console using sqlContext object. In cloudera quickstart vm, object of  HiveContext is created as sqlContext after launching spark. It will actually convert RDD to dataframe. It provide much better performance btter to running a query in Hive console.
