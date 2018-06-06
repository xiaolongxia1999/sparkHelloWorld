package spark.streaming

/**
  * Created by Administrator on 2018/4/18 0018.
  */

import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: DirectKafkaWordCount <brokers> <topics>
  *   <brokers> is a list of one or more Kafka brokers
  *   <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
  *    topic1,topic2
  */
//object DirectKafkaWordCount {
//  def main(args: Array[String]) {
//    if (args.length < 2) {
//      System.err.println(s"""
//                            |Usage: DirectKafkaWordCount <brokers> <topics>
//                            |  <brokers> is a list of one or more Kafka brokers
//                            |  <topics> is a list of one or more kafka topics to consume from
//                            |
//        """.stripMargin)
//      System.exit(1)
//    }
//
////    StreamingExamples.setStreamingLogLevels()
//
//    val Array(brokers, topics) = args
//
//    // Create context with 2 second batch interval
//    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
//    val ssc = new StreamingContext(sparkConf, Seconds(2))
//
//    // Create direct kafka stream with brokers and topics
//    val topicsSet = topics.split(",").toSet
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
////    val messages = KafkaUtils.createDirectStream(ssc,kafkaParams,topicsSet)
//    val messages = KafkaUtils.ccreateDirectStream[String, String](ssc, kafkaParams, topicsSet)
//
//    // Get the lines, split them into words, count the words and print
//    val lines = messages.map(_._2)
//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
//    wordCounts.print()
//
//    // Start the computation
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
// scalastyle:on println


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object DirectKafkaWordCount {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("KafkaDirect")//.setMaster("local[*]")
//    val ssc = new StreamingContext(conf, Seconds(10))
//    val kafkaMapParams = Map[String, Object](
////      "bootstrap.servers" -> "172.16.32.143:9092,172.16.32.142:9092,172.16.32.132:9092",
//      "bootstrap.servers" -> "biop3:9092,biop2:9092,biop4:9092",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
////      "group.id" -> "g1",
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (true: java.lang.Boolean)
//    )
////    val topicsSet = Set("ScalaTopic")
//    val topicsSet = Set("testSid")
//
//    val kafkaStream = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      PreferConsistent,
//      Subscribe[String, String](topicsSet, kafkaMapParams)
//    )
//    kafkaStream.flatMap(row => row.value().split(" ")).map((_, 1)).reduceByKey(_ + _).print()
//    ssc.start()
//    ssc.awaitTermination()



    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

//    StreamingExamples.setStreamingLogLevels()

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> brokers,
//            "bootstrap.servers" -> "172.16.32.143:9092,172.16.32.142:9092,172.16.32.132:9092",
//            "bootstrap.servers" -> "biop3:9092,biop2:9092,biop4:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "g1",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val messages = KafkaUtils.createDirectStream(
      ssc, LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,Object](topicsSet,kafkaParams))

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(x=>x.value().asInstanceOf[String])
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()








  }
}
