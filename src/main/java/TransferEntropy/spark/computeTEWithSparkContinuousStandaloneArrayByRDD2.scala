package TransferEntropy.spark

import TransferEntropy.spark.TePartitioner
import org.apache.spark.sql.expressions.Window
/**
  * Created by Administrator on 2018/5/9 0009.
  */


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import TransferEntropy.spark.TePartitioner
import TransferEntropy.spark.services.computeTE
import TransferEntropy.spark.servicesImpl._
/**
  * Created by Administrator on 2018/4/26 0026.
  */
//引入显著性检验，统计概率
object computeTEWithSparkContinuousStandaloneArrayByRDD2 {
  def main(args: Array[String]): Unit = {
    //    standalone模式
    val inputFilePath = args(0)
    val outputPath = args(1)
    val k_max = args(2).toInt
    val l_max = args(3).toInt
    val delay_max = args(4).toInt
    val pValue = args(5).toDouble

    //    val partitionNum:Int = 0

    //默认参数
    val base = 3        //base传不传入，对于连续型来说没有影响。
    val k_tau = 1
    val l_tau = 1
    //设置k,l，delay 这3个关键参数的最大值。分别取值 （0，k_max)、（0，l_max）、(0,delay_max)
    //    val k_max = 5
    //    val l_max = 5
    //    val delay_max = 5
    //    standalone模式
    val conf = new SparkConf().setAppName("TEcompute")
    val sparkContext = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()

    //    用rdd读取,csv需要是转置的，即每一行：field1,v1,v2,v3 (类似于向量形式)——————然后再collect，单机用600行Array转成600*300行Array




    //    val lines = sparkContext.textFile(inputFilePath)
    //    val array = lines.collect()
    //    val array_bf = new ArrayBuffer[String]()
    //    for(i<-0 until  array.length){
    //      for(j<-0 until array.length if i!=j){
    //        array_bf.append(array(i).split(",")(0)+"@"+array(j).split(",")(0)+"@"+array())
    //      }
    //    }

    //    还是使用rdd的cartesian操作，rdd*rdd， 自己与自己的笛卡尔积———会生成rdd.length*rdd.length个元组（ele1,ele2)——我们最后筛选掉（ele1,ele1)这种两个元素相同元素
    val schemaString = "srcToTarget k l delay teValue pValue"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)



    //    val lines = sparkContext.textFile((inputFilePath),24)
    //    val rdd = lines.map(x=>x.split(",")(0)+"@"+x.split(",").toBuffer.remove(0).toArray.mkString(","))

    val rdd = sparkContext.textFile((inputFilePath),24).map(x=>x.replaceFirst(",","@"))


    val lines_cartesian = rdd.cartesian(rdd)
    //    lines_cartesian.cache()

    //  由于cartesian数据量较大，将2个变量合并为1个，由spark去拆解，下面是以前的写法
    //    val   cartesianRDD = lines_cartesian
    //        .map(
    //            x=>( x._1.split("@")(0)+"->"+x._2.split("@")(0), utils.computeTwoSeriesTEs( x._1.split("@")(1).split(",").map(_.toDouble).toArray,x._2.split("@")(1).split(",")
    //        .map(_.toDouble).toArray,3,k_max,k_tau,l_max,l_tau,delay_max )      )).map(tuple=>utils.array3dToString(tuple._1,tuple._2).mkString("@"))//此处还是RDD[Array[String]]
    //        .flatMap(x=>x.split("@"))
    //
    //    //新生成DataFrame，分组计算Te的最大值（还需要知道其对应的是哪一行，包含k,l,delay的信息）
    //
    //    val rowRDD = cartesianRDD
    //      .map(_.split(","))
    //      .map(attributes => Row(attributes(0), attributes(1),attributes(2),attributes(3),attributes(4)))

    val   rowRDD = lines_cartesian
      .map(
        x=>( x._1.split("@")(0)+"->"+x._2.split("@")(0), utils.computeTwoSeriesTEsStringResultGaussian( x._1.split("@")(1).split(",").map(_.toDouble).toArray,x._2.split("@")(1).split(",")
          .map(_.toDouble).toArray,3,k_max,k_tau,l_max,l_tau,delay_max )      )).map(tuple=>utils.array3dToStringIncludeP(tuple._1,tuple._2).mkString("@"))//此处还是RDD[Array[String]]
      .flatMap(x=>x.split("@"))             //此处x是一个String,每个String是一条k*l*delay条X->Y，k,l,delay,teValue,pValue字符串连接而成，分隔符为@。 flatMap正是将1行转成k*l*delay行，方便生成下面的dataframe
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1),attributes(2),attributes(3),attributes(4),attributes(5)))

    // Apply the schema to the RDD
    var DF = spark.createDataFrame(rowRDD, schema)
    //除srcToTarget列外，全转成Double
    for (column<-DF.columns if !column.equalsIgnoreCase("srcToTarget")){
      DF = DF.withColumn(column,col(column).cast(DoubleType))
    }

    //    print("done!")

    //    DF.show()
    //    print(DF.count())

    //输出全部传递熵数据
    //    DF.coalesce(1).write.csv(outputPath)
    //分组输出传递熵最大值,其实就是——————分组排序（降序），取topN，此处的N=1而已
    //    DF.groupBy("srcToTarget").max("teValue").coalesce(1).write.csv(outputPath)
    //    DF.groupBy("srcToTarget").max("teValue").write.csv(outputPath)
    //    DF.orderBy("srcToTarget").groupBy("srcToTarget")

    //分组取最大值——即分组求top1
    val w = Window.partitionBy("srcToTarget").orderBy(col("teValue").desc)
    //    降序排序，取top1,见：https://www.cnblogs.com/hd-zg/p/7874291.html
    //    val dfTop1 = df.withColumn("rn", rowNumber.over(w)).where($"rn" === 1).drop("rn")
    val dfTop1 = DF.withColumn("rn",row_number().over(w)).where(col("rn") === 1).drop("rn")


    //过滤掉p值>0.05的记录，保留p<0.05的传递熵值， 阀值为1时，保留所有结果。阀值为0时，去除所有结果
    //    val pValue = 0.05
    dfTop1.where(col("pValue")<pValue).coalesce(1).write.csv(outputPath)


    //    dfTop1.coalesce(1).write.csv(outputPath)

    //      lines_cartesian.coalesce(1).saveAsTextFile(outputPath)

    //      .map(x=>
    //      (x._1.split("@")(0)+"->"+x._2.split("@"),  x._1.split("@")(1).map(_.toDouble))
    //    )
    // standalone模式，目前共24核，使用21核,并进行分区21
    //    val rdd_arr = sparkContext.parallelize(arr).map(x=>(x._1,Array(x._2,x._3))).partitionBy(new TePartitioner(21))
    //      .map(x=>(x._1,computeTwoSeriesTEs(x._2(0),x._2(1),3,k_max,k_tau,l_max,l_tau,delay_max)))
    //      .map(tuple=>utils.array3dToString(tuple._1,tuple._2).mkString("@"))
    //      .coalesce(1).saveAsTextFile(outputPath)
    //    val rdd_4d = rdd_arr.map(x=>(x._1,computeTwoSeriesTEs(x._2(0),x._2(1),3,k_max,k_tau,l_max,l_tau,delay_max)))

    //@cl，考虑用line.seperator
    //    val result = rdd_4d.map(tuple=>utils.array3dToString(tuple._1,tuple._2).mkString("@"))
    //    val result = rdd_4d.map(tuple=>utils.array3dToString(tuple._1,tuple._2).mkString(System.lineSeparator()))
    //    result.coalesce(1).saveAsTextFile(outputPath)
    //  standalone模式下，注释掉
    //    val arr4d = rdd_4d.collect()
    //    打印结果有效

    //    var count =0
    //    for(i<-0 to 50){
    //      for(k<-0 to 4){
    //        for(l<-0 to 4){
    //          for(delay<-0 to 1){
    //            println("Te pair:"+arr4d(i)._1+",k-l-delay:"+(k+1)+","+(l+1)+","+(delay+1)+",value:"+arr4d(i)._2(k)(l)(delay))
    //            count = count+1
    //          }
    //        }
    //      }
    //    }
    //    println(count)
    //    val time_spent = (System.currentTimeMillis() - startTime)/1000
    //    println("time consumed:"+time_spent)
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

