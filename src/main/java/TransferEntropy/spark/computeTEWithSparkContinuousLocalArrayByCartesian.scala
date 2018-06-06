package TransferEntropy.spark

import TransferEntropy.spark.servicesImpl.KrasKovTE
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2018/5/9 0009.
  */
object computeTEWithSparkContinuousLocalArrayByCartesian {

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    //    val inputFilePath = "E:\\bonc\\工业第二期需求\\data_sample\\单元测试数据\\datas\\DiscreteResult_state_0_1_2_withoutAllNull\\part-00000.csv"
    //    28W*14的全量离散数据
    //    val inputFilePath = "E:\\bonc\\工业第二期需求\\data_sample\\单元测试数据\\datas\\DiscreteResult_state_0_1_2\\withOutTimeAndFields.csv"
    //杨哥给的真实数据d
    //    val inputFilePath = "E:\\bonc\\工业第三期需求\\因果链路分析.csv"
    //测试数据
    val inputFilePath = "E:\\bonc\\工业第三期需求\\teDataTestTranspose.csv"
    //    val outputPath = "F:\\1\\out"
    val outputPath = "F:\\1\\out1111"

    val master = "local[*]"
    val appName = "TETest"

    //默认参数
    val base = 3
    val k_tau = 1
    val l_tau = 1
    //设置k,l，delay 这3个关键参数的最大值。分别取值 （0，k_max)、（0，l_max）、(0,delay_max)
    val k_max = 5
    val l_max = 5
    val delay_max = 5
    //local模式
    val conf = new SparkConf().setMaster(master).setAppName(appName).set("spark.driver.memory","2g")
    val sparkContext = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()


    val lines = sparkContext.textFile((inputFilePath))
    val rdd = lines.map(x=>x.split(",")(0)+"@"+x.split(",").toBuffer.remove(0).toArray.mkString(","))

    val lines_cartesian = rdd.cartesian(rdd).map(
      x=>( x._1.split("@")(0)+"->"+x._2.split("@")(0),utils.computeTwoSeriesTEs( x._1.split("@")(1).map(_.toDouble).toArray,x._2.split("@")(1).map(_.toDouble).toArray,3,k_max,k_tau,l_max,l_tau,delay_max )      )
    ).map(tuple=>utils.array3dToString(tuple._1,tuple._2).mkString("@"))
      .flatMap(x=>x.split("@"))               //一行变多行，多将


    //新生成DataFrame，分组计算Te的最大值（还需要知道其对应的是哪一行，包含k,l,delay的信息）
    val schemaString = "srcToTarget k l delay teValue"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD = lines_cartesian
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1),attributes(2),attributes(3),attributes(4)))
    // Apply the schema to the RDD
    var DF = spark.createDataFrame(rowRDD, schema)
    val colunmNames = DF.columns
    //除srcToTarget列外，全转成Double
    for (column<-DF.columns if !column.equalsIgnoreCase("srcToTarget")){
      DF = DF.withColumn(column,col(column).cast(DoubleType))
    }

    print("done!")

    DF.groupBy("srcToTarget").max("teValue").coalesce(1).write.csv(outputPath)



//    写到本地
//      .coalesce(1).saveAsTextFile(outputPath)
  }

  //对于所有序列，设定同一个（k_max,l_max,delay_max)，每两个序列之间（有序）会产生1个3维数组,数组的值为在不同的k，l,delay下的传递熵值
  //注意：数组的第一个元素的索引是0， 即从0开始，而不是从1开始。
  //因此 array3d(k)(l)(delay)的值， 对应的是 （k+1，l+1,delay+1)时的传递熵值





}
