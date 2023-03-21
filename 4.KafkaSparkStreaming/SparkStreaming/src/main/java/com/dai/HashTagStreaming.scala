package com.dai

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.matching.Regex

object HashTagStreaming {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("HashTagStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop3:9092,hadoop4:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bitcoin_consumer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Set("bitcoin"), kafkaPara))

    val valueDStream : DStream[String] = kafkaDStream.map(record => record.value())

    //5.处理队列中的 RDD 数据
    // 定义找出hashtag的正则表达式
    val pattern = new Regex("#\\w+\\b")
    val countedStream = valueDStream.flatMap(line => pattern.findAllIn(line)).map((_, 1))
    // 窗口定义，窗口12s，滑步6s
    val hashTagCounts = countedStream.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(300), Seconds(120))
    //6.打印结果
    hashTagCounts.print()
    // 开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
