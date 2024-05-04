from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, DateType, FloatType, IntegerType,ArrayType
from pyspark.sql import DataFrame
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("TMDB Dataset Processing") \
    .getOrCreate()

#Defining Schema for csv file
df_schema = StructType([
    StructField('budget', DoubleType(), True),
    StructField('genres', StringType(), True),
    StructField('homepage', StringType(), True),
    StructField('id', StringType(), True),
    StructField('keywords', StringType(), True),
    StructField('original_language', StringType(), True),
    StructField('original_title', StringType(), True),
    StructField('overview', StringType(), True),
    StructField('popularity', DoubleType(), True),
    StructField('production_companies', StringType(), True),
    StructField('production_countries', StringType(), True),
    StructField('release_date', DateType(), True),
    StructField('revenue', FloatType(), True),
    StructField('runtime', FloatType(), True),
    StructField('spoken_languages', StringType(), True),
    StructField('status', StringType(), True),
    StructField('tagline', StringType(), True),
    StructField('title', StringType(), True),
    StructField('vote_average', FloatType(), True),
    StructField('vote_count', IntegerType(), True)
])

#reading csv file from hdfs
df=spark.read.options(header=True ,schema=df_schema).csv("hdfs://namenode:9000/project/tmdb_5000_movies.csv")
df.show(5)
#caching df for performance improvement
df.cache()

#parsing json data in the table
def flatten_genres(input_df: DataFrame) -> DataFrame:
    genre_schema = ArrayType(
        StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ]
    ))
    df_parsed = input_df \
        .withColumn("genres_array", from_json(input_df["genres"], genre_schema)
    )
    df_exploded = df_parsed \
        .withColumn("genre", explode(df_parsed.genres_array)
    )
    return df_exploded \
        .withColumn("genre_id", df_exploded.genre.id) \
        .withColumn("genre_name", df_exploded["genre"]["name"]) \
        .drop("genres_array", "genre", "genres")

def flatten_keywords(input_df: DataFrame) -> DataFrame:
    keywords_schema = ArrayType(
        StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ]
    ))
    df_parsed = input_df \
        .withColumn("keywords_array", from_json(input_df["keywords"], keywords_schema)
    )
    df_exploded = df_parsed \
        .withColumn("keyword", explode(df_parsed.keywords_array)
    )
    return df_exploded \
        .withColumn("keyword_id", df_exploded.keyword.id) \
        .withColumn("keyword_name", df_exploded["keyword"]["name"]) \
        .drop("keywords_array", "keyword", "keywords")

def flatten_production_companies(input_df: DataFrame) -> DataFrame:
  production_companies_schema = ArrayType(
      StructType([
          StructField("id", IntegerType(), True),
          StructField("name", StringType(), True)
      ]
  ))

  df_parsed = input_df \
      .withColumn("production_companies_array", from_json(input_df["production_companies"], production_companies_schema)
  )

  df_exploded = df_parsed \
      .withColumn("production_company", explode(df_parsed.production_companies_array)
  )

  return df_exploded \
      .withColumn("production_company_id", df_exploded.production_company.id) \
      .withColumn("production_company_name", df_exploded["production_company"]["name"]) \
      .drop("production_companies_array", "production_company", "production_companies")

def flatten_production_countries(input_df: DataFrame) -> DataFrame:
  production_countries_schema = ArrayType(
      StructType([
          StructField("iso_3166_1", StringType(), True),
          StructField("name", StringType(), True)
      ]
  ))

  df_parsed = input_df \
      .withColumn("production_countries_array", from_json(input_df["production_countries"], production_countries_schema)
  )

  df_exploded = df_parsed \
      .withColumn("production_country", explode(df_parsed.production_countries_array)
  )

  return df_exploded \
      .withColumn("production_country_id", df_exploded.production_country.iso_3166_1) \
      .withColumn("production_country_name", df_exploded["production_country"]["name"]) \
      .drop("production_country_array", "production_country", "production_countries")

def flatten_spoken_languages(input_df: DataFrame) -> DataFrame:
   spoken_languages_schema = ArrayType(
      StructType([
          StructField("iso_639_1", StringType(), True),
          StructField("name", StringType(), True)
      ]
  ))

   df_parsed = input_df \
      .withColumn("spoken_languages_array", from_json(input_df["spoken_languages"], spoken_languages_schema)
  )

   df_exploded = df_parsed \
      .withColumn("spoken_language", explode(df_parsed.spoken_languages_array)
  )

   return df_exploded \
      .withColumn("spoken_language_id", df_exploded.spoken_language.iso_639_1) \
      .withColumn("spoken_language_name", df_exploded["spoken_language"]["name"]) \
      .drop("spoken_languages_array", "spoken_language", "spoken_languages")

output_df = df.transform(flatten_genres).transform(flatten_keywords).transform(flatten_production_companies).transform(flatten_production_countries).transform(flatten_spoken_languages)

output_df.show(20)

#Create popular_film_per_lan.csv with each original language and the most popular film in each
popular_films_df = output_df.groupBy("original_language") \
                   .agg(max("popularity").alias("popularity"),first("title").alias("most_popular_film"))
popular_films_df.show(5)
popular_films_df.write.csv("data/popular_film_per_lan.csv",   header=True)

#Create Genres_Agggregations.csv on HDFS with the id, name and number of movies for each genre
Genres_Aggregations=output_df.groupby("genre_id","genre_name").agg(count("*").alias("movies_numbers"))
Genres_Aggregations.show(5)

Genres_Aggregations.write.csv("hdfs://namenode:9000/project/Genres_Aggregations.csv",header=True)

