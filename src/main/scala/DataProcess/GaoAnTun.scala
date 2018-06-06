package DataProcess

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
/**
  * Created by Administrator on 2018/5/22 0022.
  */

//    高安屯数据，分组求平均——关键在于“时间粒度”的选取
//    另一问题是：此处可以读取文件目录下的所有文件，但是需要筛选一个时间范围——并按照此连续范围，生成一个对齐的csv
//时间范围可以先不选取， 在生成csv后，在看
object GaoAnTun {

  def main(args: Array[String]): Unit = {
//    val master = "local[*]"
//    val appName = "GaoAnTun"
////    val input_path = "F:\\BigData\\000000_1_copy_1"
    //支持通配符，正则表达式
//    val input_path = "F:\\BigData\\*_1"
//    val output_path = "F:\\BigData\\out\\"+appName+".csv"
//    val sparkConf = new SparkConf().setAppName("GaoAnTun").setMaster(master)
//    val sc = new SparkContext(sparkConf)
//    val spark = SparkSession.builder()
//        .master(master).config("spark.sql.pivotMaxValues",100000).getOrCreate()

//    分布式下
    val input_path = args(0)
    val output_path = args(1)
    val interval = args(2).toDouble         //可以考虑先用300s， 即5分钟。

    val sparkConf = new SparkConf().setAppName("GaoAnTun")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config("spark.sql.pivotMaxValues",100000).getOrCreate()

//    时间粒度的定义策略， 直接影响到传递熵的时滞长度
//    val colTime = substring_index(col("time"),":",2)        //此处是截取到分钟,但是空值太多
//    另一个时间粒度——按10分钟，截取字符串的1-2              //但是太特殊，不如按照通用情形——时间粒度（unix_time/时间粒度间隔）
//    val colTime =  substring(col("time"),1,15)      //也有可能是0-14，
//    val interval = 5*60     //求平均的时间粒度，单位：秒

//    val rdd = spark.sparkContext.textFile(input_path)
    var df = spark.read.format("csv").option("header",false).csv(input_path)
    val scheamString = "id group chinese value time".split(" ")
    println(df.columns.mkString(","))
    for(i<-0 to df.columns.length-1){
      df = df.withColumnRenamed(df.columns(i),scheamString(i))
    }

//    @cl:分组求平均的关键在于,time列，要转成什么样的“时间粒度”
    //使用functions内置函数的字符串功能：切割——substring_index的第3个参数，是从1开始，而不是从0开始————此处是按分钟求平均
    df = df.select("id","value","time").withColumn("value",col("value").cast(DoubleType))
//          .withColumn("time",colTime)
              .withColumn("longTime",round(unix_timestamp(col("time"))/interval,0))      //增加时间粒度列(unix时间/粒度，并精确到个位，方便分组




//    df.show()
//    println(df.count())

    //df筛选某个时间范围，并排序

//    var df1 = df.groupBy("id").pivot("time").avg("value")    //得到的dataframe就是第一列为索引名，其他各列就是时间戳。
      var df1 = df.groupBy("id").pivot("longTime").avg("value")
//                .na.fill(9999)
//    df1.na.fill(9999)
//      df1.show()
//      println(df1.count())
      //需要为true，不然不知道每一行的样本时间，是否已经排完序
    df1.coalesce(1).write.mode(SaveMode.Overwrite).option("header",true).csv(output_path)

  }

}
