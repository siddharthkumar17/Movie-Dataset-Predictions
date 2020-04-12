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
val merged = credits.join(ratings, ratings("movieId")===credits("id"), "inner").join(keywords,Seq("id"), "inner" ).join(movies_metadata, Seq("id"),"inner").drop("movieId").withColumn("id", credits("id").cast(LongType))



def extractActorList(cast: String): String ={
  val temp = cleanString(cast)
  var res: String = ""
  val json = new Gson().fromJson(temp, classOf[JsonArray]);
  for(t<-0 to json.size()-1)
    res=res+json.get(t).getAsJsonObject().get("name").getAsString().replace("\"", "").replace(" ","-")+' '
  return res.trim()
}

val myudf = udf((x: String) => extractActorList(x))

val m1 = merged.withColumn("cast",myudf($"cast"))
