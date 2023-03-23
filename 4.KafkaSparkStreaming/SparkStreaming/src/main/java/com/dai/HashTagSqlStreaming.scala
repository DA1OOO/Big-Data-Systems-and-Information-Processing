package com.dai

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HashTagSqlStreaming {
  def main(args: Array[String]): Unit = {
    val pattern = "#\\w+\\s"
    val spark = SparkSession.builder().appName("HashTagStreamingCount").getOrCreate()
    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "hadoop3:9092,hadoop3:9092")
      .option("subscribe", "bitcoin").load().selectExpr("CAST(value AS STRING) as message")
    // 给列带上时间戳和tag，方便进行窗口函数
    val time_df = df.select("message.*").withColumn("timestamp", current_timestamp()).withColumn("hashtag", regexp_extract(col("message"), pattern, 0))
    // 窗口计数
    val counts = time_df
      .groupBy(
        window(col("timestamp"), "5 minutes", "2 minutes"),
        col("hashtag")
      ).count().orderBy(col("window"))
    val query = counts.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()
  }
}
