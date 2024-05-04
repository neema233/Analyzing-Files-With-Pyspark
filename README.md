# analyzing-files-with-pyspark
This project analyzes a movie dataset from TMDB containing information on almost 5,000 movies. The data is stored in a CSV file with various attributes about each movie, including genres represented as an array of JSON objects.

# Benefits of Pre-Aggregations:

This project utilizes pre-aggregation tables to improve query performance. When dealing with large datasets, where reads are significantly more frequent than writes, pre-aggregating data can save substantial processing time. By pre-computing certain aggregations and storing them in separate tables, subsequent queries that require those aggregations can access the pre-calculated results instead of re-computing them from the raw data every time.
# Setting up Environment 
Using Docker image for hadoop and spark Detailes [Here](https://github.com/neema233/docker-hadoop-spark)

Access namenode for HDFS
```
  docker exec -it namenode bash
```

Access spark-master for pyspark
```
  docker exec -it spark-master bash

  /spark/bin/pyspark --master spark://spark-master:7077
```
# Processing the TMDB Dataset:
[***CODE***](https://github.com/neema233/analyzing-files-with-pyspark/blob/main/hdfs%26pyspark.py) && [***DataFiles***](https://github.com/neema233/analyzing-files-with-pyspark/tree/main/Data)

***1- SparkSession and Schema:***

A SparkSession is created for working with distributed data using Apache Spark.
A schema is defined to specify the data types for each column in the CSV file.

***2- Loading and Caching Data:***

The movie data is loaded from HDFS using Spark's CSV reader with the defined schema and header information.
The DataFrame is cached in memory for faster processing of subsequent operations.

***3- Parsing JSON Genres:***

A function flatten_genres parses the JSON array in the "genres" column.
It extracts individual genre objects (including "id" and "name") and creates separate rows for each genre associated with a movie.
Similar functions (flatten_keywords, etc.) are used to process other JSON columns (keywords, production companies, production countries, and spoken languages).

***4- Exploding Genre Data:***

The explode function is used to transform the single "genres" column containing an array of JSON objects into separate rows, one for each genre within a movie.

***5- Generating Pre-Aggregations:***

 - ***Genres Aggregation***:
The DataFrame is grouped by "genre_id" and "genre_name".
The count function is used to aggregate the number of movies for each genre, creating a pre-aggregated table named "Genres_Aggregations.csv" stored on HDFS.
-  ***Most Popular Film per Language***:
The DataFrame is grouped by "original_language".
The max function is used to find the movie with the highest "popularity" within each language group.
The first function is used to retrieve the title of the most popular film (handling cases where a language might have no movies).
The resulting DataFrame named "popular_films_df" is written to a local CSV file "popular_film_per_lan.csv".

