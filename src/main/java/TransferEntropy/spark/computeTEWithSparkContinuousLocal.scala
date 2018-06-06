package TransferEntropy.spark

/**
  * Created by Administrator on 2018/4/27 0027.
  */
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

import scala.collection.mutable.ArrayBuffer
import TransferEntropy.spark.TePartitioner
import TransferEntropy.spark.services.computeTE
import TransferEntropy.spark.servicesImpl._
/**
  * Created by Administrator on 2018/4/26 0026.
  */
//考虑使用spark dataframe操作，语法更简单，但是切片操作，不知道是否符合两列的传递熵计算预期。
object computeTEWithSparkContinuousLocal {
  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()
    //    val inputFilePath = "E:\\bonc\\工业第二期需求\\data_sample\\单元测试数据\\datas\\DiscreteResult_state_0_1_2_withoutAllNull\\part-00000.csv"
    //    28W*14的全量离散数据
    //    val inputFilePath = "E:\\bonc\\工业第二期需求\\data_sample\\单元测试数据\\datas\\DiscreteResult_state_0_1_2\\withOutTimeAndFields.csv"

    //杨哥给的真实数据d
//    val inputFilePath = "E:\\bonc\\工业第三期需求\\因果链路分析.csv"
    //测试数据
    val inputFilePath = "E:\\bonc\\工业第三期需求\\因果链路分析test.csv"
    //    val outputPath = "F:\\1\\out"
    val outputPath = "F:\\1\\out1"

//    standalone模式
//    val inputFilePath = args(0)
//    val outputPath = args(1)


    //离散型设置为IntergerType, 连续型设置为DoubleType
    //此处有这些种类：`string`, `boolean`, `byte`, `short`, `int`, `long`,
    //    * `float`, `double`, `decimal`, `date`, `timestamp`.
    val dataType = "double"
//    local模式
    val master = "local[*]"
    val appName = "TETest"
    val partitionNum:Int = 0

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
//    standalone模式
//    val conf = new SparkConf().setAppName("TEcompute")

    val sparkContext = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()

    var df  = spark.read.format("csv").option("header",true).csv(inputFilePath)

    for(column<-df.columns){
//      df = df.withColumn(column,col(column).cast(dataType))
      df = df.withColumn(column,col(column).cast(DoubleType))
    }
    //    df.cache()

//    df.show(20)

    //    var  arr = new ArrayBuffer[(String,Array[String],Array[String])]()
    //arr是Array[Tuple3]
    var  arr = new ArrayBuffer[(String,Array[Double],Array[Double])]()
    //    var rdd:RDD[Array[String]] = sparkContext.parallelize(Array(Array()))

    //cl：getInt后面，对于连续型传递熵，要改成getDouble
//    注意，此处是在driver中跑，是单机的
    for(col1<-df.columns){
      for(col2<-df.columns if col2 != col1){
        val arr_a = df.select(col1).rdd.map(x=>x.getDouble(0)).collect()
        val arr_b = df.select(col2).rdd.map(x=>x.getDouble(0)).collect()
        arr.append((col1+col2,arr_a,arr_b))     //
        //        arr.append(arr_b)
      }
    }
//local模式
//    val rdd_arr = sparkContext.parallelize(arr).map(x=>(x._1,Array(x._2,x._3))).partitionBy(new TePartitioner(3))
// standalone模式，目前共24核，使用21核
    val rdd_arr = sparkContext.parallelize(arr).map(x=>(x._1,Array(x._2,x._3))).partitionBy(new TePartitioner(21))



    val rdd_4d = rdd_arr.map(x=>(x._1,computeTwoSeriesTEs(x._2(0),x._2(1),3,k_max,k_tau,l_max,l_tau,delay_max)))


    //@cl，考虑用line.seperator
    val result = rdd_4d.map(tuple=>utils.array3dToString(tuple._1,tuple._2).mkString("@"))
//    val result = rdd_4d.map(tuple=>utils.array3dToString(tuple._1,tuple._2).mkString(System.lineSeparator()))

    result.coalesce(1).saveAsTextFile(outputPath)

    //  standalone模式下，注释掉
    val arr4d = rdd_4d.collect()

    //    arr4d的结构—— 是一个 14*13的Array[Tuple2()]
    //打印arr4d的内部各层集合的长度，没问题
    //    println(arr4d(0)._2(0)(0)(0))
    //    println(arr4d.length)
    //    println(arr4d(0)._2.length)
    //    println(arr4d(0)._2.length)
    //    println(arr4d(0)._2(0).length)


    //    打印结果有效
    //  standalone模式下，注释掉
    var count =0
    for(i<-0 to 50){
      for(k<-0 to 4){
        for(l<-0 to 4){
          for(delay<-0 to 1){
            println("Te pair:"+arr4d(i)._1+",k-l-delay:"+(k+1)+","+(l+1)+","+(delay+1)+",value:"+arr4d(i)._2(k)(l)(delay))
            count = count+1
          }
        }
      }
    }
    //    println(count)
    val time_spent = (System.currentTimeMillis() - startTime)/1000
    println("time consumed:"+time_spent)
  }

  //对于所有序列，设定同一个（k_max,l_max,delay_max)，每两个序列之间（有序）会产生1个3维数组,数组的值为在不同的k，l,delay下的传递熵值
  //注意：数组的第一个元素的索引是0， 即从0开始，而不是从1开始。
  //因此 array3d(k)(l)(delay)的值， 对应的是 （k+1，l+1,delay+1)时的传递熵值


  //计算离散情形
  def computeTwoSeriesTEs(src:Array[Double],dest:Array[Double],base:Int,k_max:Int,k_tau:Int,l_max:Int,l_tau:Int,delay_max:Int):Array[Array[Array[Double]]] ={
    var array3d = Array.ofDim[Double](k_max,l_max,delay_max)
    for(k<-1 to k_max){
      for(l<-1 to l_max){
        for(delay<-1 to delay_max){
//          println("ok!")
          val calc11 = new KrasKovTE(k,k_tau,l,l_tau,delay).computeContinuous(src,dest)
//          println("ok1!")
          array3d(k-1)(l-1)(delay-1) = calc11  //此处计算出来的是java.lang.Double,但是存储却用的scala.Double，可能报错
        }
      }
    }
    array3d
  }

  //3维的Array转成Array[String],便于存储到本地
  //  以utils里的同名方法为准
  def array3dToString(key:String,arr:Array[Array[Array[Double]]]):Array[String]={
    //    val arrayLast = Array.ofDim(arr.length*arr(0).length*arr(0)(0).length)
    val arrayBuffer = new ArrayBuffer[String]()
    for(k<-0 until arr.length){
      for(l<-0 until arr(0).length){
        for(delay<-0 until arr(0)(0).length){
          //          arrayBuffer.append(""+key+k+","+l+","+delay+arr(k)(l)(delay)+System.lineSeparator())
          arrayBuffer.append(""+key+","+k+","+l+","+delay+","+arr(k)(l)(delay)+System.lineSeparator())
        }
      }
    }
    arrayBuffer.toArray
  }
}