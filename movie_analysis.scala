import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._
import com.google.gson._
import org.apache.spark.sql.types._
import scala.collection.mutable._
def cleanString(input: String): String = {

  return input.replace('\'','\"').replace("None","\"\"")

}

val spark: SparkSession = SparkSession.builder.appName("movies").getOrCreate()

val credits: DataFrame = spark.read.format("csv") .option("header", "true").load("/FileStore/tables/credits.csv")
val ratings: DataFrame = spark.read.format("csv").option("header", "true").load("/FileStore/tables/ratings.csv").drop("userId", "timestamp").groupBy("movieId")
  .agg(avg("rating").as("avg_rating"), min("rating").as("min_rating"),max("rating").as("max_rating")).withColumnRenamed("movieId","id")
val keywords: DataFrame = spark.read.format("csv").option("header", "true").load("/FileStore/tables/keywords.csv")
val movies_metadata: DataFrame = spark.read.format("csv").option("header", "true").load("/FileStore/tables/movies_metadata.csv")
var merged = credits.join(ratings, Seq("id"), "inner").join(keywords,Seq("id"), "inner" ).join(movies_metadata, Seq("id"),"inner")
merged = merged.drop("tagline","spoken_languages", "vote_count", "vote_average", "homepage", "imdb_id", "production_countries", "poster_path", "original_title")
merged = merged.withColumn("budget", merged("budget").cast(LongType))
def extractActorList(cast: String): Array[String] ={
  val temp = cleanString(cast)
  var res: String = ""
  val json = new Gson().fromJson(temp, classOf[JsonArray]);
  for(t<-0 to json.size()-1)
    res=res+json.get(t).getAsJsonObject().get("name").getAsString().replace("\"", "").replace(" ","-")+' '
  return res.trim().split(" ")
}

val udf1 = udf((x: String) => extractActorList(x))
merged = merged.withColumn("cast",udf1($"cast")).withColumn("keywords",udf1($"keywords"))

def extractCrew(crew: String): Map[String,String] ={
  val temp = cleanString(crew)
  var res = new HashMap[String, String]
  val json = new Gson().fromJson(temp, classOf[JsonArray]);
  for(t<-0 to json.size()-1){
    val a = json.get(t).getAsJsonObject()
    res+=(a.get("job").getAsString() -> a.get("name").getAsString().replace("\"", "").replace(" ","-"))
  }
  
  
  return res
}

val udf2 = udf((x: String) => extractCrew(x))
merged = merged.withColumn("crew",udf2($"crew"))
