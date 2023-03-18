package com.dai

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.substring
object NineYearCrimeDataProcess {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Crime_Analysis")
    //    val conf: SparkConf = new SparkConf().setAppName("Crime_Analysis")
    //import spark.implicits._
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val data_2010 = spark.read.option("header", value = true).csv("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\4.KafkaSparkStreaming\\data\\Crime_Incidents_in_2010.csv")
    val data_2011 = spark.read.option("header", value = true).csv("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\4.KafkaSparkStreaming\\data\\Crime_Incidents_in_2011.csv")
    val data_2012 = spark.read.option("header", value = true).csv("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\4.KafkaSparkStreaming\\data\\Crime_Incidents_in_2012.csv")
    val data_2013 = spark.read.option("header", value = true).csv("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\4.KafkaSparkStreaming\\data\\Crime_Incidents_in_2013.csv")
    val data_2014 = spark.read.option("header", value = true).csv("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\4.KafkaSparkStreaming\\data\\Crime_Incidents_in_2014.csv")
    val data_2015 = spark.read.option("header", value = true).csv("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\4.KafkaSparkStreaming\\data\\Crime_Incidents_in_2015.csv")
    val data_2016 = spark.read.option("header", value = true).csv("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\4.KafkaSparkStreaming\\data\\Crime_Incidents_in_2016.csv")
    val data_2017 = spark.read.option("header", value = true).csv("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\4.KafkaSparkStreaming\\data\\Crime_Incidents_in_2017.csv")
    val data_2018 = spark.read.option("header", value = true).csv("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\4.KafkaSparkStreaming\\data\\Crime_Incidents_in_2018.csv")
    val mergedData = data_2010.union(data_2011).union(data_2012).union(data_2013).union(data_2014).union(data_2015).union(data_2016).union(data_2017).union(data_2018)
    val timeMethod = mergedData.select(substring(mergedData("REPORT_DAT"),0, 4) as("YEAR"), mergedData("METHOD"))
    // 计算总crime数
    val yearlyCrimeNums = timeMethod.groupBy("YEAR").count().orderBy("YEAR")
    // 修改count列名
    val newYearlyCrimeNums = yearlyCrimeNums.select(yearlyCrimeNums("YEAR") as ("YEAR_"), yearlyCrimeNums("count") as("total_crime_count"))
    // 计算gun crime次数
    val yearlyGunCrimeNums = timeMethod.groupBy("YEAR", "METHOD").count().select("YEAR","count").where("METHOD == 'GUN'")
    // 将两表join
    val joined_table = newYearlyCrimeNums.join(yearlyGunCrimeNums, newYearlyCrimeNums("YEAR_") === yearlyGunCrimeNums("YEAR"), "inner")
    // 计算percentage
    val result = joined_table.select(joined_table("YEAR"), joined_table("count") / joined_table("total_crime_count") as("gun_percentage")).orderBy("YEAR")
    result.show()
    spark.close()
  }
}