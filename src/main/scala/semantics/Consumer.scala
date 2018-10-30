package sparkstreaming

import java.time.format.DateTimeFormatter
object MyUtils {
  val format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
}
import MyUtils._
import java.time.{LocalDate, LocalDateTime, ZoneId, ZonedDateTime}
import java.time.temporal.ChronoUnit

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import java.util.Calendar

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
import org.apache.commons.lang.time.DateUtils
import semantics.TweetTransformer
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Scanner


object Consumer {
  def main(args: Array[String]) {
    println("Set Micro-Batch Interval time (integer) (1, 5, 15, 30, 60) -min: ")
    val scanner = new Scanner(System.in)
    val interval = scanner.nextInt()
    println("Choose Nasdaq stock i.e. TSLA, MSFT")
    val stockName = scanner.next()

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS tweetstock_space WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS tweetstock_space.batch_avg (timegen timestamp PRIMARY KEY,tuplevalue tuple<text, double, double>);")

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TweetAvg")
    val ssc = new StreamingContext(sparkConf, Minutes(interval))
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

    val tweetStream = TweetTransformer.createTweetSemantics(ssc, stockName).cache()
    //timezones
    val nyZone = ZoneId.of("America/New_York")
    val sthlmZone = ZoneId.of("Europe/Stockholm")

    val stockByTime = stockStream.map{ case (value, time) =>
      val dateTime = LocalDateTime.parse(time,format)
      val nyTime = ZonedDateTime.of(dateTime,nyZone)
      (nyTime.withZoneSameInstant(sthlmZone),value.replaceAll("\"","").toDouble)}

    //date format
    // times are batched to ceil(time % 5)

    val tweetByTime = tweetStream.map{
        case (time, (stock , avg)) =>
          val sthlmTime = time.toInstant().atZone(sthlmZone)
          val sthlmTrunc = sthlmTime.truncatedTo(ChronoUnit.HOURS)
                                    .plusMinutes((sthlmTime.getMinute - (sthlmTime.getMinute % interval)) % 60)
          (sthlmTrunc,(stock,avg))
      }

    //stockByTime.print()
    tweetByTime.print()

    val joinedStream = stockByTime.join(tweetByTime)
    val parsedTimeStream = joinedStream.map{
      case (time,(value,(stock,semanticAvg))) => (time.toLocalDateTime.toString,(stock,value,semanticAvg))
    }.cache()
    parsedTimeStream.print()
    // store the result in Cassandra
    parsedTimeStream.saveToCassandra("tweetstock_space", "batch_avg", SomeColumns("timegen", "tuplevalue"))
    // run
    ssc.start()
    ssc.awaitTermination()
  }
}

