package otherTest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/3/26 0026.
  */
object ReadFile {
  def main(args: Array[String]): Unit = {

    val path = "E:\\bonc\\工业第二期需求\\data_sample\\data.txt"
    val conf = new SparkConf().setAppName("readTxt").setMaster("local[8]")
    val sc =new SparkContext(conf)

    val start = System.currentTimeMillis()
    val rdd = sc.textFile(path)
    println(rdd.count())
    println(rdd.collect().take(100))
    val end = System.currentTimeMillis()
    println(end - start)
  }
}
