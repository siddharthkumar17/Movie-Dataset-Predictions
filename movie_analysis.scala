import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._


val spark: SparkSession = SparkSession.builder.appName("movies").getOrCreate()

val credits: DataFrame = spark.read.format("csv") .option("header", "true").load("/FileStore/tables/credits.csv")
val ratings: DataFrame = spark.read.format("csv").option("header", "true").load("/FileStore/tables/ratings.csv")
val keywords: DataFrame = spark.read.format("csv").option("header", "true").load("/FileStore/tables/keywords.csv")
val movies_metadata: DataFrame = spark.read.format("csv").option("header", "true").load("/FileStore/tables/movies_metadata.csv")
val merged = credits.join(ratings, credits("id")===ratings("movieId"), "inner").join(keywords,credits("id")===keywords("id"), "inner" ).join(movies_metadata, credits("id")===movies_metadata("id"),"inner")

merged.show()
merged.printSchema()