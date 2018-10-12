name := "Twitter-Semantic-Analysis"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.1",
  "org.apache.spark" %% "spark-streaming" % "2.3.1",
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1"
)