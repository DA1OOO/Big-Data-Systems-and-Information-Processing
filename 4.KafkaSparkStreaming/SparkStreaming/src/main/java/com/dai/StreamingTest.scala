package com.dai

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.matching.Regex

object StreamingTest {
  def main(args: Array[String]): Unit = {
    //1.初始化 Spark 配置信息
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")
    //2.初始化 SparkStreamingContext 4s一个批次处理一次数据
    val ssc = new StreamingContext(conf, Seconds(1))
    //3.创建 RDD 队列
    val rddQueue = new mutable.Queue[RDD[String]]()
    //4.创建 QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue,oneAtATime = false)
    //5.处理队列中的 RDD 数据
    // 定义找出hashtag的正则表达式
    val pattern = new Regex("#\\w+\\b")
    val countedStream = inputStream.flatMap(line => pattern.findAllIn(line)).map((_, 1)).reduceByKey(_ + _)
    // 窗口定义，窗口12s，滑步6s
    val hashTagCounts = countedStream.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(12), Seconds(6))
    //6.打印结果
    hashTagCounts.print()
    //7.启动任务
    ssc.start()
    //8.循环创建并向 RDD 队列中放入 RDD
    for (i <- 1 to 5) {
//      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      rddQueue += ssc.sparkContext.makeRDD(List("RT @tyler: Goldman Sachs is getting into #bitcoin https://t.co/ZGW2miEXdT,2021-03-31 19:46:11"))
      rddQueue += ssc.sparkContext.makeRDD(List("no FOMO, en 15 minutos posiblemente se forme la 3ª divergencia bajista en grafico de 4 horas#bitcoin #btc https://t.co/fgAyoqiFi5,2021-03-31 19:46:17"))
      Thread.sleep(4000)

    }
    ssc.awaitTermination()
  }
}
