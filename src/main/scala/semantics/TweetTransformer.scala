package semantics

import java.util.Date

import edu.stanford.nlp.pipeline.StanfordCoreNLP
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import semantics.SentimentAnalyzer._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
/** Simple application to listen to a stream of Tweets and print them out */
object TweetTransformer {

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
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }



  def createTweetSemantics(ssc: StreamingContext, stockName: String): DStream[(Date, (String, Double))] = {
    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    //val ssc = new StreamingContext("local[2]", "PrintTweets", Seconds(30))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    val filters = Seq(stockName)
    val twitterStream = TwitterUtils.createStream(ssc, None, filters)
    val relevantTweets = twitterStream.filter(tweet => tweet.getLang == "en")
    val sentiment = relevantTweets.map{ tweet =>
      (tweet,SentimentAnalyzer.mainSentiment(tweet.getText),tweet.getCreatedAt)
    }
    //simplified for one stock only
    //no need to double filter for one stock
    val keyTweetSentiment = sentiment.map { tweet =>
      // val string = tweet._1.getText.toLowerCase
      // var key = "None"
      // filters.foreach(e => if (string.contains(e.toLowerCase)) {
      //   key = e
      // })
      (stockName, (tweet._2, 1, tweet._3))
    }

    //time is the last tweet
    val sentimentAvg = keyTweetSentiment.reduceByKey{case ((sentL,countL, _),(sentR,countR, timeR)) =>
      (sentL + sentR, countL + countR, timeR)
    }
      .map{
        case (stock, (sum , count, time)) => (time, (stock, sum.toDouble / count.toDouble))
      }
    return sentimentAvg
    // val filters = Seq("AAPL", "TSLA", "Nasdaq", "Kardashian")
    // val twitterStream = TwitterUtils.createStream(ssc, None, filters)
    // val relevantTweets = twitterStream.filter(tweet => tweet.getLang == "en")
    //                                   .filter(tweet => !tweet.isRetweet)
    // val sentiment = relevantTweets.map{ tweet =>
    //   (tweet,SentimentAnalyzer.mainSentiment(tweet.getText))
    // }
    // //can be improved
    // val keyTweetSentiment = sentiment.map { tweet =>
    //   val string = tweet._1.getText.toLowerCase
    //   var key = "None"
    //   filters.foreach(e => if (string.contains(e.toLowerCase)) {
    //     key = e
    //   })
    //   (key, (tweet._2, 1))
    // }.cache()

    // keyTweetSentiment.print()

    // val sentimentAvg = keyTweetSentiment.reduceByKey{case ((sentL,countL),(sentR,countR)) => (sentL + sentR, countL + countR)}
    //     .mapValues{
    //       case (sum , count) => sum.toDouble / count.toDouble
    //    }
    // return sentimentAvg

  }
}

