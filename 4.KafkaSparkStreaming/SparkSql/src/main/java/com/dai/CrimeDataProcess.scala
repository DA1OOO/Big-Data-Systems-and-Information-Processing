package com.dai

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CrimeDataProcess {
  def main(args: Array[String]): Unit = {
    //    val conf : SparkConf = new SparkConf().setMaster("local[*]").setAppName("Crime_Analysis")
    val conf: SparkConf = new SparkConf().setAppName("Crime_Analysis")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //    val file = spark.read.option("header",true).csv("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\4.KafkaSparkStreaming\\data\\Crime_Incidents_in_2013.csv")
    val file = spark.read.option("header", true).csv("/data/Crime_Incidents_in_2013.csv")
    // task a
    val filteredData = file.select("CCN", "REPORT_DAT", "OFFENSE", "METHOD", "END_DATE", "DISTRICT").filter("END_DATE is not null and CCN is not null and REPORT_DAT is not null and OFFENSE is not null and METHOD is not null and DISTRICT is not null")
    filteredData.write.csv("/data/Filtered_Crime_Incidents_in_2013.csv")
    // task b
    val offenseCount = file.groupBy("OFFENSE").count()
    offenseCount.write.csv("/data/offenseCount.csv")
    val timeCount = file.groupBy("SHIFT").count()
    timeCount.write.csv("/data/timeCount.csv")
    spark.close()
  }
}
