# Real Time User Events Tracker
# Stream Handler

This is a Python+Scala application that generates mock real time user events and pushes to Kafka topic and handles streaming data from it and writes it to Cassandra. It uses Apache Spark and Apache Cassandra libraries for data processing and storage.

## Prerequisites

Before running this application, make sure you have the following:

- Apache Spark installed and configured on your system. You can install Apache Spark using Homebrew (macOS) or manually.

  **Homebrew (macOS)**:
  ```shell
  brew install apache-spark
  ```

  **Manual Installation**:
  - Download Apache Spark from the official website: [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html)
  - Extract the downloaded archive and follow the installation instructions in the provided documentation.

- Apache Cassandra installed and running. You can install Apache Cassandra using Homebrew (macOS) or manually.

  **Homebrew (macOS)**:
  ```shell
  brew install cassandra
  ```

  **Manual Installation**:
  - Download Apache Cassandra from the official website: [http://cassandra.apache.org/download](http://cassandra.apache.org/download)
  - Follow the installation instructions provided in the downloaded documentation.

- Kafka broker running with the specified bootstrap servers and the topic `app_events` available. You can install Kafka using Homebrew (macOS) or manually.

  **Homebrew (macOS)**:
  ```shell
  brew install kafka
  ```

  **Manual Installation**:
  - Download Kafka from the official website: [https://kafka.apache.org/downloads](https://kafka.apache.org/downloads)
  - Extract the downloaded archive and follow the installation instructions in the provided documentation.

## Setup

1. Clone the repository or download the source code containing the snippet.

2. Ensure that you have the required dependencies in your build file or build tool configuration. The necessary dependencies include:
   - Apache Spark
   - Apache Cassandra
   - Kafka
   - Python3

3. Update the Kafka bootstrap servers and topic in the `StreamHandler` object:
   ```scala
   val kafkaBootstrapServers = "localhost:9092"
   val kafkaTopic = "app_events"
   ```

4. Configure the Spark session to connect to your Cassandra instance by updating the host in the following line of code:
   ```scala
   .config("spark.cassandra.connection.host", "localhost")
   ```

5. Build the project using sbt.

## Generating Events

Go to terminal and do ```./user_events.py```
This will start generating events and pushing to kafka topic

## Running the Application (Kafka -> Cassandra)

Once you have completed the setup steps, you can run the application by following these instructions:

1. Start Apache Cassandra if it's not already running.

   **Homebrew (macOS)**:
   ```shell
   brew services start cassandra
   ```

   **Manual Installation**:
   - Start Cassandra using the appropriate command for your installation.

2. Launch the Kafka broker with the specified bootstrap servers and make sure the `app_events` topic exists.

   **Homebrew (macOS)**:
   - Start ZooKeeper:
     ```shell
     brew services start zookeeper
     ```

   - Start Kafka:
     ```shell
     brew services start kafka
     ```

   **Manual Installation**:
   - Start ZooKeeper and Kafka using the appropriate commands for your installation.

3. Execute the compiled code.

Do: a. CD StreamHandler
    b. sbt package && spark-submit --class StreamHandler --master 'local[*]' --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,com.datastax.cassandra:cassandra-driver-core:3.11.3" ./target/scala-2.12/stream-handler_2.12-1.0.jar

4. The application will start consuming data from the Kafka topic and writing it to Cassandra. You will see log messages indicating the progress.

5. To stop the application, press Ctrl+C.

Note: You can customize the processing time interval by modifying the value in the `trigger` method:
```scala
.trigger(Trigger.ProcessingTime("5 seconds"))
```

## Creating Kafka Topic

If the `app_events` topic does not exist in your Kafka cluster, you can create it using the following commands:

**Homebrew (macOS)**:
```shell
# Start ZooKeeper (if not already running)
brew services start zookeeper

# Start Kafka (if not already running)
brew services start kafka

# Create the topic
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic app_events
```

## Aggregating on raw data to get Final Output (Cassandra -> Final Output)

Run:
sbt package && spark-submit --class TransformHandler --master 'local[*]' --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,com.datastax.cassandra:cassandra-driver-core:3.11.3" ./target/scala-2.12/stream-handler_2.12-1.0.jar

## Additional Notes

- If we want to start consuming data from the beginning of the Kafka topic, uncomment the following line of code:
  ```scala
  // .option("startingOffsets", "earliest")
  ```
- We can apply any transformation logic in TransformHandler file and send data to s3 and create dashboards


## Screenshots:
<img width="1177" alt="Screenshot 2023-07-15 at 18 57 59" src="https://github.com/suryansh314/realTimeUserEvents/assets/69847943/5481b8c3-ab1e-41a1-a633-562dbc043a4a">

1. Event generator and pushes to kafka

<img width="1440" alt="Screenshot 2023-07-15 at 19 00 46" src="https://github.com/suryansh314/realTimeUserEvents/assets/69847943/51509b6d-62de-4f22-aba9-95f156e1c51b">
<img width="1403" alt="Screenshot 2023-07-15 at 19 00 30" src="https://github.com/suryansh314/realTimeUserEvents/assets/69847943/de07cd6d-8c1a-4295-862a-ab25a4164541">

2. Spark submit and UI

<img width="1179" alt="Screenshot 2023-07-15 at 19 02 49" src="https://github.com/suryansh314/realTimeUserEvents/assets/69847943/d62ff6ff-ef7d-443c-8339-c35ad7efa39d">

3. Cassandra table records

<img width="1388" alt="Screenshot 2023-07-15 at 19 04 05" src="https://github.com/suryansh314/realTimeUserEvents/assets/69847943/a8924438-e5d5-49dc-b886-807a6426781d">

4. Transform handler spark submit

<img width="1176" alt="Screenshot 2023-07-15 at 19 05 56" src="https://github.com/suryansh314/realTimeUserEvents/assets/69847943/bcc7deee-d461-48b3-bff2-825a47776fbc">

5. Final aggregated CSV generated




