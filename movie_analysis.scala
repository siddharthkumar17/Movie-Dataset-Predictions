import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

val spark: SparkSession = SparkSession.builder.appName("movies").getOrCreate()

val df: DataFrame = spark.read.csv("/FileStore/tables/links.csv")
df.show()

