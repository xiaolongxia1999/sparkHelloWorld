package commonUtils

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
/**
  * Created by Administrator on 2018/5/23 0023.
  */
object dataframeUtils {

  def selectCols(df1:DataFrame,array:Array[String]):DataFrame={
    val cols = array.map(x=>col(x))
    val df2 = df1.select(cols:_*)       //这个语法是没问题的

    df2
  }

  def main(args: Array[String]): Unit = {

    val inputs_path = "E:\\bonc\\工业第三期需求\\杨哥给的数据\\高安屯数据3点5T\\utilTest.csv"
    val inputs_map = "E:\\bonc\\工业第三期需求\\杨哥给的数据\\高安屯数据3点5T\\高安屯因果链路分析点表.csv"
    val spark = SparkSession.builder().master("local[*]").appName("hh").config("spark.sql.pivotMaxValues",100000).getOrCreate()



    val df = spark.read.option("header",true).csv(inputs_path)
    val arr = spark.read.option("header",true).csv(inputs_map).select(col("sid")).rdd.map(_.getString(0)).collect()
//    val arr = Array("10P-11.UNIT1@NET1#0","10VBT-11.UNIT1@NET1#1")
//    println(arr.length)
    df.show()

    //之前总报，有问题：cannot resolve XXX.就是因为arr中一部分字段名，是df里没有的。 df找不到对应的列名，才报错
    //我以为杨哥的3450列，完全包含在我的测试数据里。 后来发现，有6列不是。
    val df222 = this.selectCols(df,arr)
//    val df222 = df.select("10P#11#UNIT1#NET1#0","10VBT#11#UNIT1#NET1#1")
//    df.select(df.columns.apply(0),df.columns.apply(1)).show()
    df222.show()
    println(df222.count())
    println(df222.columns.length)
    println(df.columns.length)

  }
}
