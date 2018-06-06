package TransferEntropy.spark

import TransferEntropy.spark.newUtils.{computeTwoSeriesTEsStringResultGaussian, computeTwoSeriesTEsStringResultGaussianSrcName, findBestArmaP}
import org.apache.spark.sql.expressions.Window

import scala.collection.mutable
/**
  * Created by Administrator on 2018/5/9 0009.
  */
import com.cloudera.sparkts.models.ARIMA.autoFit

import TransferEntropy.spark.services.computeTE
import TransferEntropy.spark.servicesImpl._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2018/4/26 0026.
  */
//引入显著性检验，统计概率
object computeTEWithSparkContinuousStandaloneArrayByRDD4 {
  def main(args: Array[String]): Unit = {
    //    standalone模式
    val inputFilePath = args(0)
    val outputPath = args(1)
    val k_max = args(2).toInt
    val l_max = args(3).toInt
    val delay_max = args(4).toInt
    val pValue = args(5).toDouble

    val maxP = 10
    val maxD = 2
    val maxQ = 10
    //    val partitionNum:Int = 0

    //默认参数
    val base = 3        //base传不传入，对于连续型来说没有影响。
    val k_tau = 1
    val l_tau = 1
    var model:computeTE = new GaussianTE()
    //设置k,l，delay 这3个关键参数的最大值。分别取值 （0，k_max)、（0，l_max）、(0,delay_max)
    //    val k_max = 5
    //    val l_max = 5
    //    val delay_max = 5
    //    standalone模式
    val conf = new SparkConf().setAppName("TEcompute")   //.setJars(Array("sparkts-0.4.0-SNAPSHOT-jar-with-dependencies.jar"))
    val sparkContext = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()

    val parallelism = 24

    //    用rdd读取,csv需要是转置的，即每一行：field1,v1,v2,v3 (类似于向量形式)——————然后再collect，单机用600行Array转成600*300行Array

    //    还是使用rdd的cartesian操作，rdd*rdd， 自己与自己的笛卡尔积———会生成rdd.length*rdd.length个元组（ele1,ele2)——我们最后筛选掉（ele1,ele1)这种两个元素相同元素
    val schemaString = "srcToTarget k l delay teValue pValue"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    //    val lines = sparkContext.textFile((inputFilePath),24)
    //    val rdd = lines.map(x=>x.split(",")(0)+"@"+x.split(",").toBuffer.remove(0).toArray.mkString(","))

    val rdd = sparkContext.textFile((inputFilePath),parallelism)
    rdd.cache()

    val rdd1 = rdd.map(x=>x.replaceFirst(",","@"))
    val lines_cartesian = rdd1.cartesian(rdd1).repartition(parallelism)


  //时序定阶————注意Vector是scala自带的类型，Vector则是spark中自定义的类（非类型),且vector类所在包，有静态类Vectors，勿混淆
    val map_ts_k = rdd1.map( x=>
      //这一句定阶：如果ARIMA中差分也无法平稳，则默认为K_MAX，只计算K_MAX的值
//      ( x.split("@")(0),findBestArmaP(Vectors.dense(x.split("@")(1).map(_.toDouble).toArray),maxP,maxD,maxQ,k_max   )         )      //findBestArmaP( DenseVector,maxP,maxD,maxQ )
      //由于ARIMA考虑到时序的平稳性，若在maxD内都不平稳，会报错。为了定阶，AR就够了，可以考虑maxD和maxQ定阶为0
      //k_max为防止上面ARIMA报错，给个默认的k值输出
      ( x.split("@")(0),findBestArmaP(Vectors.dense(x.split("@")(1).map(_.toDouble).toArray),maxP,0,0,k_max   )         )
    ).collect()



    val hashMap = new mutable.HashMap[String,Int]()
    map_ts_k.map(x => hashMap.put(x._1,x._2))

    print("stage hashMap done!")

    //计算传递熵矩阵（未定阶）
    val rowRDD = lines_cartesian
//            生成 4元元组 （srcName, srcSeries,destName,destSeries) ,其中，srcName,destName为String类型，表示source和target的变量名
//              srcSeries和destSeries是Array[Double]类型， 作为传递熵值计算的输入，两个 double[]类型的序列
            .map(x=> ( x._1.split("@")(0),x._1.split("@")(1).split(",").map(_.toDouble).toArray , x._2.split("@")(0) , x._2.split("@")(1).split(",").map(_.toDouble).toArray ))
            .map( x => (x._1+"->"+x._3 , computeTwoSeriesTEsStringResultGaussianSrcName (x._1,hashMap,x._2,x._4 ,3,k_max,k_tau,l_max,l_tau,delay_max) ) )
            .flatMap(y =>  y._2.map( result =>(y._1,result)) )
//            .flatMap(y => y._2.map(b=> y._1+","+b )    )
            .map(x=> Array(x._1,x._2).mkString(","))    //此处全部用“，”分隔，后面构造dataframe需要

//    rowRDD.saveAsTextFile(outputPath)
//    print("done!")
//     rowRDD.coalesce(1).saveAsTextFile(outputPath)
//            .map(attributes => Row(attributes._1, attributes._2(0),attributes._2(1),attributes._2(2),attributes._2(3),attributes._2(4)))

            .map(_.split(","))
            .map(attributes => Row(attributes(0), attributes(1),attributes(2),attributes(3),attributes(4),attributes(5)))

//    cl
    // Apply the schema to the RDD
    var DF = spark.createDataFrame(rowRDD, schema)
    //除srcToTarget列外，全转成Double
    for (column<-DF.columns if !column.equalsIgnoreCase("srcToTarget")){
      DF = DF.withColumn(column,col(column).cast(DoubleType))
    }
    //分组取最大值——即分组求top1
    val w = Window.partitionBy("srcToTarget").orderBy(col("teValue").desc)
    //    降序排序，取top1,见：https://www.cnblogs.com/hd-zg/p/7874291.html
    //    val dfTop1 = df.withColumn("rn", rowNumber.over(w)).where($"rn" === 1).drop("rn")
    val dfTop1 = DF.withColumn("rn",row_number().over(w)).where(col("rn") === 1).drop("rn")
    //过滤掉p值>0.05的记录，保留p<0.05的传递熵值， 阀值为1时，保留所有结果。阀值为0时，去除所有结果
    //    val pValue = 0.05
    dfTop1.where(col("pValue")<pValue).coalesce(1).write.csv(outputPath)

//    cl

  }

  //对于所有序列，设定同一个（k_max,l_max,delay_max)，每两个序列之间（有序）会产生1个3维数组,数组的值为在不同的k，l,delay下的传递熵值
  //注意：数组的第一个元素的索引是0， 即从0开始，而不是从1开始。
  //因此 array3d(k)(l)(delay)的值， 对应的是 （k+1，l+1,delay+1)时的传递熵值


  //计算连续情形
  def computeTwoSeriesTEs(src:Array[Double],dest:Array[Double],base:Int,k_max:Int,k_tau:Int,l_max:Int,l_tau:Int,delay_max:Int):Array[Array[Array[Double]]] ={
    var array3d = Array.ofDim[Double](k_max,l_max,delay_max)
    var calc11 = new KrasKovTE()
    var result = 0.0
    for(k<-1 to k_max){
      for(l<-1 to l_max){
        for(delay<-1 to delay_max){
          //          println("ok!")
          //          val calc11 = new KrasKovTE(k,k_tau,l,l_tau,delay).computeContinuous(src,dest)
          calc11.setK(k)
          calc11.setK_tau(k_tau)
          calc11.setL(l)
          calc11.setL_tau(l_tau)
          calc11.setDelay(delay)
          result = calc11.computeContinuous(src,dest)         //这里接收Array[Double]，但该方法在java代码里则是接收double[]
          array3d(k-1)(l-1)(delay-1) = result
          //          println("ok1!")
          //          array3d(k-1)(l-1)(delay-1) = calc11  //此处计算出来的是java.lang.Double,但是存储却用的scala.Double，可能报错
        }
      }
    }
    array3d
  }

  //原方法——会new很多对象，应该复用对象
  //  def computeTwoSeriesTEs(src:Array[Double],dest:Array[Double],base:Int,k_max:Int,k_tau:Int,l_max:Int,l_tau:Int,delay_max:Int):Array[Array[Array[Double]]] ={
  //    var array3d = Array.ofDim[Double](k_max,l_max,delay_max)
  //
  //    for(k<-1 to k_max){
  //      for(l<-1 to l_max){
  //        for(delay<-1 to delay_max){
  //                    println("ok!")
  //                    val calc11 = new KrasKovTE(k,k_tau,l,l_tau,delay).computeContinuous(src,dest)
  //                    println("ok1!")
  //          array3d(k-1)(l-1)(delay-1) = calc11  //此处计算出来的是java.lang.Double,但是存储却用的scala.Double，可能报错
  //        }
  //      }
  //    }
  //    array3d
  //  }
  //
  //

  //自定义函数，如果java方便，直接在java里面编写，否则会涉及到java和scala之间的类型转换，容器转换。
  //包含显著性统计概率，返回Array[Array[Array[Double]]]
  def computeTwoSeriesTEsSringResult(src:Array[Double],dest:Array[Double],base:Int,k_max:Int,k_tau:Int,l_max:Int,l_tau:Int,delay_max:Int):Array[Array[Array[String]]] ={
    var array3d = Array.ofDim[String](k_max,l_max,delay_max)
    var calc11 = new KrasKovTE()
    //    var result = ""
    for(k<-1 to k_max){
      for(l<-1 to l_max){
        for(delay<-1 to delay_max){
          //          println("ok!")
          //          val calc11 = new KrasKovTE(k,k_tau,l,l_tau,delay).computeContinuous(src,dest)
          calc11.setK(k)
          calc11.setK_tau(k_tau)
          calc11.setL(l)
          calc11.setL_tau(l_tau)
          calc11.setDelay(delay)
          //此处采用了默认的100次permutation,可以自行定义
          array3d(k-1)(l-1)(delay-1) = k+","+l+","+delay+","+calc11.computeContinuous(src,dest,100)         //这里接收Array[Double]，但该方法在java代码里则是接收double[]

          //          println("ok1!")
          //          array3d(k-1)(l-1)(delay-1) = calc11  //此处计算出来的是java.lang.Double,但是存储却用的scala.Double，可能报错
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

          arrayBuffer.append(""+key+","+(k+1)+","+(l+1)+","+(delay+1)+","+arr(k)(l)(delay)+System.lineSeparator())

        }
      }
    }
    arrayBuffer.toArray
  }
}