import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CrimeDataProcess {
  def main(args: Array[String]): Unit = {
    val conf : SparkConf = new SparkConf().setMaster("local[*]").setAppName("Crime_Analysis")
    val spark : SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val file = spark.read.option("header",true).csv("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\4.KafkaSparkStreaming\\data\\Crime_Incidents_in_2013.csv")
    file.select("CCN", "REPORT_DAT", "OFFENSE", "METHOD", "END_DATE", "DISTRICT").limit(10).show()

    spark.close()

  }
}
