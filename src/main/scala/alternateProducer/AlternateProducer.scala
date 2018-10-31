package alternateProducer

import java.util.{Date, Properties, Calendar}
import scala.util.{Try, Success, Failure}
import java.lang.Math
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import kafka.producer.KeyedMessage
import org.apache.http.client.fluent.Request
import scala.util.Try
import play.api.libs.json._
import java.util.Scanner

object AlternateProducer extends App {
  //we realized Vantage API can be horribly delayed
  //here's a backup
  val scanner = new Scanner(System.in)
  println("Set Micro-Batch Interval time (integer) (1, 5, 15, 30, 60) -min: ")
  val interval = scanner.nextInt
  println("Choose Nasdaq stock i.e. TSLA, MSFT")
  val stockName = scanner.next.toLowerCase

  val topic = "avgSemantics"
  val brokers = "localhost:9092"
  val rnd = new Random()
  var previousTimestamp = ""

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "ScalaProducerExample")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)
  var runAgain = true
  while(true){
    var now = Calendar.getInstance()
    var currentSecond = now.get(Calendar.SECOND)
    while(runAgain){
      val baseUri = "https://api.iextrading.com/1.0/stock/"
      val function = "/chart/1d"

      val jsonString = Try(
        Request
          .Get(s"$baseUri$stockName$function")
          .execute()
          .returnContent()
          .toString()
      ).getOrElse("0")
      //println("string: " + jsonString)

      val json = Json.parse(jsonString).asInstanceOf[JsArray]
      //println("json: " + json)
        val stockElement = json.value.last
        //println("key: " + stockElement)
        //create same syntax as alpha vantage
        val stockValue = (stockElement \ "close").get.asInstanceOf[JsNumber].value.toInt
        val stockMin = (stockElement \ "minute").get.toString()
        val actualMin = stockMin.substring(4, stockMin.length - 1).toInt
        val wait = actualMin % interval
        if (wait == 0) {
          var stockDate = (stockElement \ "date").get.toString()
          stockDate =  stockDate.substring(1,5) + ":" + stockDate.substring(5,7) + ":" + stockDate.substring(7,9)
          val stockKey = stockDate + " " + stockMin.substring(1, stockMin.length - 1) + ":00"
          val kafkaEntry = stockValue + "," + stockKey
          if (previousTimestamp != kafkaEntry) {
            val data = new ProducerRecord[String, String](topic, null, kafkaEntry)
            previousTimestamp = kafkaEntry
            print("\t" + stockKey + "\n")
            producer.send(data)
            now = Calendar.getInstance()
            currentSecond = now.get(Calendar.SECOND)
            Thread.sleep((60000 * interval) - currentSecond)
          }
        }
        else{
          now = Calendar.getInstance()
          currentSecond = now.get(Calendar.SECOND)
          Thread.sleep((60000 * wait) - currentSecond)
        }
    }
  }

  producer.close()
}

