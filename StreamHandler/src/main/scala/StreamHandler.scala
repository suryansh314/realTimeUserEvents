import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.cassandra._

import com.datastax.oss.driver.api.core.uuid.Uuids
import com.datastax.spark.connector._

case class Event(timestamp: Double, userId: String, city_name: String, step: String)

object StreamHandler {
  def main(args: Array[String]) {
    val kafkaBootstrapServers = "localhost:9092"
    val kafkaTopic = "app_events"

    // initialize Spark
    val spark = SparkSession
      .builder
      .appName("Stream Handler")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()

    import spark.implicits._

    val eventsDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
//      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map { eventString =>
        val Array(timestamp, userId, city_name, step) = eventString.split(",")
        Event(timestamp.toDouble, userId, city_name, step)
      }
      .toDF()


    val query = eventsDF
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
        println(s"Writing to Cassandra $batchID")
        batchDF
          .select("userid","city_name","timestamp","step")
          .write
          .cassandraFormat("user_events", "events") // table, keyspace
          .mode("append")
          .save()
      }
      .outputMode("update")
      .start()

    // until ^C
    query.awaitTermination()
  }
}
