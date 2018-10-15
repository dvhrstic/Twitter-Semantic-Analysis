package semantics

import edu.stanford.nlp.pipeline.StanfordCoreNLP
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import semantics.SentimentAnalyzer._
import org.apache.spark.streaming.StreamingContext._
/** Simple application to listen to a stream of Tweets and print them out */
object PrintTweets {

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  /** Configures Twitter service credentials using twiter.txt in the main workspace directory */
  def setupTwitter() = {
    val stream = getClass.getResourceAsStream("/twitter-keys.txt")
    val lines = scala.io.Source.fromInputStream( stream ).getLines
    lines.foreach
    { line =>
      println(line)
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }



  def main(args: Array[String]) {
    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    val ssc = new StreamingContext("local[2]", "PrintTweets", Seconds(30))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    val filters = Seq("AAPL", "TSLA", "Nasdaq", "Kardashian")
    val twitterStream = TwitterUtils.createStream(ssc, None, filters)
    val RelevantTweets = twitterStream.filter(tweet => tweet.getLang == "en")
                                      .filter(tweet => !tweet.isRetweet)
    val sentiment = RelevantTweets.map{ tweet =>
      (tweet,SentimentAnalyzer.mainSentiment(tweet.getText))
    }
    //can be improved
    val keyTweetSentiment = sentiment.map { tweet =>
      val string = tweet._1.getText.toLowerCase
      var key = "None"
      filters.foreach(e => if (string.contains(e.toLowerCase)) {
        key = e
      })
      (key, (tweet._2, 1))
    }.cache()

    keyTweetSentiment.print()

    val SentimentAvg = keyTweetSentiment.reduceByKey{case ((sentL,countL),(sentR,countR)) => (sentL + sentR, countL + countR)}
        .mapValues{
          case (sum , count) => sum.toDouble / count.toDouble
        }.print()





    // val sentimentRatio = specificTopic.foreachRDD{
    //   (rdd, time) =>
    //     val total = rdd.count().toDouble
    //     val negative = rdd.filter(tweet => tweet._2 == "NEGATIVE").count().toDouble
    //     if( total != 0.0){
    //       println(negative / total)
    //     }
    //     else(println(total))
    // }
    // Kick it all off
    ssc.start()
    ssc.awaitTermination()
  }
}
