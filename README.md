import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by huang on 16-1-12.
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Wow,My first spark app!").setMaster("spark://192.168.3.210:7077").
            set("spark.driver.host","192.168.3.217").setJars(List("home/huang/mapeng/TextSpark/out/artifacts/TextSpark_jar/TextSpark.jar"))

    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://192.168.3.210:9000/user/hive/warehouse/ods_gasfachourdata/month_id=201501/GasFacHourData_201501_22579840.txt", 1)

    val wordCounts = lines.flatMap {x => x.split("\t")}.map((_,1)).reduceByKey(_ + _).map {x => (x._2,x._1)}.sortByKey(false).map(x => (x._2, x._1))

    wordCounts.collect.foreach(println)

    sc.stop()
  }
}
