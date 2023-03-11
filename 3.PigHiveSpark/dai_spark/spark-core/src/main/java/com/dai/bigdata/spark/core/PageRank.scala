package com.dai.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PageRank {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("PageRank-2").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    //分区数 2 / 8 / 16
    val partitionNum = 16
    // 读取文件
    // 也可以是HDFS路径
    val fileRDD: RDD[String] = sc.textFile("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\3.PigHiveSpark\\data\\pagerank-data\\web-Google.txt", partitionNum).persist()

    // 将page分隔后，生成rank记录表 保存rank分数，每行为一个page，rank默认为1
    val pageRDD: RDD[String] = fileRDD.flatMap(_.split("\t"))
    var ranks: RDD[(String, Double)] = pageRDD.distinct().map {
      (_, 1.0)
    }
    // 将每一列构建成一组k-v对: from - List(To)
    val pagePair = fileRDD.map { line =>
      val token = line.split("\t")
      (token(0), List(token(1)))
    }.distinct()

    // 按fromPage为key进行聚合，value通过:::进行List的追加
    val links: RDD[(String, List[String])] = pagePair.reduceByKey(_ ::: _).partitionBy(new HashPartitioner(10)).persist()
    for (_ <- 0 until 10) {
      val contributions: RDD[(String, Double)] = links.join(ranks).flatMap {
        case (_, (links, rank)) =>
          links.map(dest => (dest, rank / links.size))
      }
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
    }
    // 结果按value的降序保存到文件
    ranks.sortBy(f => f._2, false).saveAsTextFile("result")
  }
}