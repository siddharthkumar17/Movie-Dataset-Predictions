import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._
import com.google.gson._
import org.apache.spark.sql.types._

def cleanString(input: String): String = {

  return input.replace('\'','\"').replace("None","\"\"")

}

val spark: SparkSession = SparkSession.builder.appName("movies").getOrCreate()

val credits: DataFrame = spark.read.format("csv") .option("header", "true").load("/FileStore/tables/credits.csv")
val ratings: DataFrame = spark.read.format("csv").option("header", "true").load("/FileStore/tables/ratings.csv")
val keywords: DataFrame = spark.read.format("csv").option("header", "true").load("/FileStore/tables/keywords.csv")
val movies_metadata: DataFrame = spark.read.format("csv").option("header", "true").load("/FileStore/tables/movies_metadata.csv")
var merged = credits.join(ratings, ratings("movieId")===credits("id"), "inner").join(keywords,Seq("id"), "inner" ).join(movies_metadata, Seq("id"),"inner")
merged = merged.drop("movieId","userId","tagline","spoken_languages", "vote_count", "vote_average", "homepage", "imdb_id", "production_countries", "poster_path", "original_title", "timestamp")
merged = merged.withColumn("rating", merged("rating").cast(DoubleType)).withColumn("budget", merged("budget").cast(LongType))
def extractActorList(cast: String): Array[String] ={
  val temp = cleanString(cast)
  var res: String = ""
  val json = new Gson().fromJson(temp, classOf[JsonArray]);
  for(t<-0 to json.size()-1)
    res=res+json.get(t).getAsJsonObject().get("name").getAsString().replace("\"", "").replace(" ","-")+' '
  return res.trim().split(" ")
}

val udf1 = udf((x: String) => extractActorList(x))
merged = merged.withColumn("cast",udf1($"cast"))

def extractCrew(crew: String): Array[String] ={
  val temp = cleanString(crew)
  var res: String = ""
  val json = new Gson().fromJson(temp, classOf[JsonArray]);
  for(t<-0 to json.size()-1){
    val a = json.get(t).getAsJsonObject()
    res=res+a.get("job").getAsString() +'-'+ a.get("name").getAsString().replace("\"", "").replace(" ","-")+' '
  }
  res = res.trim()
  
  return res.split(" ")
}

val udf2 = udf((x: String) => extractCrew(x))
merged = merged.withColumn("crew",udf2($"crew"))

print(merged.first())