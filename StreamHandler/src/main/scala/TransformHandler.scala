import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object TransformHandler {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CassandraToCSVJob")
      .master("local[*]")
      .config("spark.cassandra.connection.host", "localhost")  // Replace with your Cassandra host
      .getOrCreate()

    // Read data from Cassandra table
    val cassandraTable = "user_events"
    val df = spark.read.format("org.apache.spark.sql.cassandra")
      .option("table", cassandraTable)
      .option("keyspace", "events")  //Cassandra keyspace
      .load()

    // Perform aggregation stage wise
    val stepPercntAgg = df.groupBy("step")
      .count()
      .withColumn("percentage", expr("count * 100.0 / sum(count) over ()"))

    // Write the result to a CSV file
    stepPercntAgg
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("/Users/suryanshsingh/stepPercntAgg.csv")

    //city wise aggregation
    val cityUserCountDF = df.groupBy("city_name")
      .agg(count("userid").alias("user_count"))

    val totalUserCount = df.select("userid").count()

    val cityUserWiseDF = cityUserCountDF.withColumn("percentage", col("user_count") * 100.0 / totalUserCount)

    cityUserWiseDF.show(false)

    cityUserWiseDF
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("/Users/suryanshsingh/cityUserWiseAgg.csv")

    // Stop the Spark session
    spark.stop()
  }
}
