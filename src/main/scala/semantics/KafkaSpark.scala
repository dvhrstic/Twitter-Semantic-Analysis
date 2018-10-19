package sparkstreaming

import java.util.{Calendar, Date, HashMap, Properties}

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import java.util.Date
import org.apache.spark.storage.StorageLevel
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Cluster, Host, Metadata, Session}
import com.datastax.spark.connector.streaming._
import semantics.PrintTweets
import org.apache.log4j.Logger
import org.apache.log4j.Level


object KafkaSpark {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    // connect to Cassandra and make a keyspace and table as explained in the document
    // val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    // val session = cluster.connect()
    // session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    // session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordAvg")
    val ssc = new StreamingContext(sparkConf, Seconds(100))
    ssc.checkpoint("file:///tmp/spark/checkpoint")
    val topic = "avg".split(",").toSet
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")

    val messages = KafkaUtils.createDirectStream[String, String,StringDecoder, StringDecoder](ssc, kafkaConf, topic)
    val stockStream = messages.map(element => {
      val currElement = element._2.split(",")
      (currElement(0),currElement(1))
    } ).cache()

    val tweetStream = PrintTweets.createTweetSemantics(ssc).cache()
    //tweetStream.print()

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val stockByTime = stockStream.map{ case (value, time) => (value, format.parse(time))}

    val tweetByTime = tweetStream.transform(
      (rdd, time) => {
        rdd.map(
          (time, _)
        )
      }
    )

    stockByTime.print()
    tweetByTime.print()

    // store the result in Cassandra
    //stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))
    // Now we run the data flow
    ssc.start()
    ssc.awaitTermination()
  }
}

