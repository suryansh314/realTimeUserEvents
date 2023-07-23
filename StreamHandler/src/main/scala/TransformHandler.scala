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

    df.write.mode(SaveMode.Overwrite).option("header", "true").csv("/Users/suryanshsingh/df.csv")

    val totalDistinctUserIds = df.agg(countDistinct("userid")).head.getLong(0)

    // aggregation stage wise
    val stepPercntAgg = df
      .withColumn("rn",expr("ROW_NUMBER() OVER (PARTITION BY userid ORDER BY timestamp DESC) AS rn"))
      .filter("rn=1")
      .groupBy("step")
      .agg(round(countDistinct("userid")*100/lit(totalDistinctUserIds),2).alias("cnt_prcnt"))
      .cache()

    stepPercntAgg.show(100,0)

    stepPercntAgg
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("/Users/suryanshsingh/stepPercntAgg.csv")

    //city wise aggregation
    val cityUserCountDF = df.groupBy("city_name")
      .agg(countDistinct("userid").alias("user_count")).cache()

    cityUserCountDF.show(100,false)

    cityUserCountDF
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("/Users/suryanshsingh/cityUserWiseAgg.csv")

    spark.stop()
  }
}
