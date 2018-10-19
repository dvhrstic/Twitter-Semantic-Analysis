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


object ScalaProducerExample extends App {

    val alphabet = 'a' to 'z'
    val events = 10000
    val topic = "avg"
    val brokers = "localhost:9092"
    val rnd = new Random()

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
        .Get(s"$baseUri?function=$function&symbol=$symbol&interval=1min&outputsize=$outputSize&apikey=$key")
        .execute()
        .returnContent()
        .toString()
        ).getOrElse("0")

        val json = Json.parse(jsonString).as[JsObject]
        val timeseries = Try(json("Time Series (1min)"))

        timeseries match {
            case Success(e) =>
                val keys = timeseries.get.as[JsObject].keys.toSeq
                val stockValue = (timeseries.get.as[JsObject]\ keys(1) \ "4. close").get.toString()
                val kafkaEntry = stockValue + "," + keys(1).toString
                val data = new ProducerRecord[String, String](topic, null, kafkaEntry)
                producer.send(data)
                //print("\t" + data + "\n")
                runAgain = false
                success = true
            case Failure(v) =>
                print(" No data from Stock API \n")
                runAgain = true
                success = false
                // delay 5 sec
                val prevSec = Calendar.getInstance().get(Calendar.SECOND) % 10
                print("\t Wait untill a new API call \n")
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
