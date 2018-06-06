package DataProcess

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
/**
  * Created by Administrator on 2018/5/25 0025.
  */
object teValueFilter {
  def main(args: Array[String]): Unit = {

    val input_path = "E:\\bonc\\工业第三期需求\\杨哥给的数据\\高安屯数据3点5T\\全量结果的传递熵\\out-pp_result3t_transpose-0524-1847-1-1-1.csv"
    val output_path = "E:\\bonc\\工业第三期需求\\杨哥给的数据\\高安屯数据3点5T\\全量结果的传递熵\\out"

    val sparkConf = new SparkConf().setAppName("GaoAnTun").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config("spark.sql.pivotMaxValues",100000).getOrCreate()

    var df = spark.read.option("header",false).csv(input_path)

    val scheamString = "id k l delay value pValue".split(" ")
    println(df.columns.mkString(","))
    for(i<-0 to df.columns.length-1){
      df = df.withColumnRenamed(df.columns(i),scheamString(i))
    }

    df.withColumn("pValue",col("pValue").cast(DoubleType))

    val  df1 = df.where(col("pValue")<0.01)
    df1.coalesce(1).write.option("header",false).csv(output_path)
    println(df1.count())

  }
}
