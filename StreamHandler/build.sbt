name := "Stream Handler"

version := "1.0"

scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.3.0",
  "com.datastax.cassandra" % "cassandra-driver-core" % "4.0.0"
)
