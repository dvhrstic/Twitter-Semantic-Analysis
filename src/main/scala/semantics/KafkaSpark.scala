package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")
    
    // make a connection to Kafka and read (key, value) pairs from it
    // local[2] - one thread for receiving and one for the application
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordAvg")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("file:///tmp/spark/checkpoint")
    val topic = "avg".split(",").toSet
    val kafkaConf = Map(
        "metadata.broker.list" -> "localhost:9092",
        "zookeeper.connect" -> "localhost:2181",
        "group.id" -> "kafka-spark-streaming",
        "zookeeper.connection.timeout.ms" -> "1000")

    val messages = KafkaUtils.createDirectStream[String, String,StringDecoder, StringDecoder](ssc, kafkaConf, topic)
    val pairs = messages.map(element => { 
        val currElement = element._2.split(",")
        (currElement(0),currElement(1).toDouble) 
      } )

  //  measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
	    val newValue = value.getOrElse(0.0)
      val oldAvg = state.getOption.getOrElse(0.0)
      val avg = (newValue + oldAvg) / 2
      state.update(avg)
      (key, avg)
    }
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))
    // store the result in Cassandra
    stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))
    // Now we run the data flow
    ssc.start()
    ssc.awaitTermination()
  }
}
