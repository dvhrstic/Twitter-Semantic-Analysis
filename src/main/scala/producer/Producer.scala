package producer

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

object ScalaProducerExample extends App {
    println("Set Micro-Batch Inverval time (1, 5, 30) -min: ")
    val scanner = new Scanner(System.in)
    val intervalTime = scanner.nextInt()  + "min"

    val topic = "avg"
    val brokers = "localhost:9092"
    val rnd = new Random()
    var previousTimestamp = ""

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "ScalaProducerExample")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)


    var now = Calendar.getInstance()
    var currentSecond = now.get(Calendar.SECOND) % 10
            var runAgain = false
            var success = false
            while(true){

                while(Math.abs(Calendar.getInstance().get(Calendar.SECOND) % 10 - currentSecond).toInt == 5 | runAgain == true | success == false) {

                    val baseUri = "https://www.alphavantage.co/query"
                    val key = "APIKEY"
                    val outputSize = "compact"
                    val symbol = "MSFT"
                    val function = "TIME_SERIES_INTRADAY"


                    val jsonString = Try(
                        Request
                          .Get(s"$baseUri?function=$function&symbol=$symbol&interval=$intervalTime&outputsize=$outputSize&apikey=$key")
                          .execute()
                          .returnContent()
        .toString()
        ).getOrElse("0")

        val json = Json.parse(jsonString).as[JsObject]
        val timeseries = Try(json("Time Series ("+ intervalTime + ")"))

        timeseries match {
            case Success(e) =>
                val stockKey = timeseries.get.as[JsObject].keys.toSeq(0)
                val stockValue = (timeseries.get.as[JsObject]\ stockKey \ "4. close").get
                val kafkaEntry = stockValue + "," + stockKey
                if(!kafkaEntry.equals(previousTimestamp)) {
                  val data = new ProducerRecord[String, String](topic, null, kafkaEntry)
                  producer.send(data)
                  previousTimestamp = kafkaEntry
                  print("\t" + stockKey + "\n")
                }
                runAgain = false
                success = true
            case Failure(v) =>
                print(" No data from Stock API \n")
                runAgain = true
                success = false
                // delay 5 sec
                val prevSec = Calendar.getInstance().get(Calendar.SECOND) % 10
                print("\t Wait until a new API call \n")
                while(Math.abs(Calendar.getInstance().get(Calendar.SECOND) % 10 - prevSec).toInt != 5){
                }
                val waitingTime = Math.abs(Calendar.getInstance().get(Calendar.SECOND) % 10 - prevSec).toInt 
                print(s"\t Waiting done in $waitingTime seconds \n")
        }

        now = Calendar.getInstance() 
        currentSecond = now.get(Calendar.SECOND) % 10
        //print(now.get(Calendar.SECOND) + "\n")
        }
    }
    
    producer.close()
}
