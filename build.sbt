name := "Twitter-Semantic-Analysis"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "org.apache.spark" %% "spark-sql" % "2.2.1",
  "org.apache.spark" % "spark-streaming_2.10" % "2.2.1",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.10" % "2.2.1",
  "org.twitter4j" % "twitter4j-core" % "4.0.3",
  "org.twitter4j" % "twitter4j-stream" % "4.0.3"
)