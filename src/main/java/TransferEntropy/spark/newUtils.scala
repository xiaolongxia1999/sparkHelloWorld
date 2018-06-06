package TransferEntropy.spark

import scala.collection.mutable.HashMap

import TransferEntropy.JavaUtils.JavaUtils
import TransferEntropy.spark.servicesImpl.{GaussianTE, KrasKovTE}

import scala.collection.mutable.ArrayBuffer
import java.lang._
//import java.util._

import TransferEntropy.spark.services.computeTE
//import com.cloudera.sparkts.models.ARIMA.autoFit
import com.cloudera.sparkts.models.ARIMA.findBestARMAModel

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import com.cloudera.sparkts.models._
/**
  * Created by Administrator on 2018/4/26 0026.
  */
object newUtils extends Serializable{
  def array3dToString(key: String, arr: Array[Array[Array[scala.Double]]]): Array[String] = {
    //    val arrayLast = Array.ofDim(arr.length*arr(0).length*arr(0)(0).length)
    val arrayBuffer = new ArrayBuffer[String]()
    for (k <- 0 until arr.length) {
      for (l <- 0 until arr(0).length) {
        for (delay <- 0 until arr(0)(0).length) {
          arrayBuffer.append("" + key + "," + (k + 1) + "," + (l + 1) + "," + (delay + 1) + "," + arr(k)(l)(delay)) //+System.lineSeparator())，展示不需要换行符
        }
      }
    }
    arrayBuffer.toArray
  }

  //包含显著性检验信息
  def array3dToStringIncludeP(key: String, arr: Array[Array[Array[String]]]): Array[String] = {
    //    val arrayLast = Array.ofDim(arr.length*arr(0).length*arr(0)(0).length)
    val arrayBuffer = new ArrayBuffer[String]()
    for (k <- 0 until arr.length) {
      for (l <- 0 until arr(0).length) {
        for (delay <- 0 until arr(0)(0).length) {
          arrayBuffer.append("" + key + "," + arr(k)(l)(delay)) //+System.lineSeparator())，展示不需要换行符
        }
      }
    }
    arrayBuffer.toArray
  }

  //计算连续情形
  def computeTwoSeriesTEs(src: Array[scala.Double], dest: Array[scala.Double], base: Int, k_max: Int, k_tau: Int, l_max: Int, l_tau: Int, delay_max: Int): Array[Array[Array[scala.Double]]] = {
    var array3d = Array.ofDim[scala.Double](k_max, l_max, delay_max)
    var calc11 = new KrasKovTE()
    var result = 0.0
    //scala的ArrayBuffer转java的List,List再转double[]
    //这里src和dest都要由scala的Array[Double]转成double
    import scala.collection.JavaConverters._
    import scala.collection.JavaConversions._
    val listSrc: java.util.List[String] = src.map(x => x.toString).toBuffer.asJava
    val srcArray = JavaUtils.convertToJavaArray(listSrc)
    val listDest: java.util.List[String] = dest.map(x => x.toString).toBuffer.asJava
    val destArray = JavaUtils.convertToJavaArray(listDest)

    for (k <- 1 to k_max) {
      for (l <- 1 to l_max) {
        for (delay <- 1 to delay_max) {
          //          println("ok!")
          //          val calc11 = new KrasKovTE(k,k_tau,l,l_tau,delay).computeContinuous(src,dest)
          calc11.setK(k)
          calc11.setK_tau(k_tau)
          calc11.setL(l)
          calc11.setL_tau(l_tau)
          calc11.setDelay(delay)

          //          println("srcArray ele:"+srcArray.mkString(","))
          //          println("destArray ele:"+srcArray.mkString(","))
          result = calc11.computeContinuous(srcArray, destArray)
          //          这个result最后还要由java中的double转成Array中的Double类。
          array3d(k - 1)(l - 1)(delay - 1) = String.valueOf(result).toDouble
          //          array3d(k-1)(l-1)(delay-1) = result
          //          println("ok1!")
          //          array3d(k-1)(l-1)(delay-1) = calc11  //此处计算出来的是java.lang.Double,但是存储却用的scala.Double，可能报错
        }
      }
    }
    array3d
  }


  //将两个序列的传递熵结果，使用3维的String数组保存——String是java和scala数据交换的重要方式，不需要转来转去
  def computeTwoSeriesTEsStringResult(src: Array[scala.Double], dest: Array[scala.Double], base: Int, k_max: Int, k_tau: Int, l_max: Int, l_tau: Int, delay_max: Int): Array[Array[Array[String]]] = {
    var array3d = Array.ofDim[String](k_max, l_max, delay_max)
    var calc11 = new KrasKovTE()
    //    var result = 0.0
    //scala的ArrayBuffer转java的List,List再转double[]
    //这里src和dest都要由scala的Array[Double]转成double
    import scala.collection.JavaConverters._
    import scala.collection.JavaConversions._
    val listSrc: java.util.List[String] = src.map(x => x.toString).toBuffer.asJava
    val srcArray = JavaUtils.convertToJavaArray(listSrc)
    val listDest: java.util.List[String] = dest.map(x => x.toString).toBuffer.asJava
    val destArray = JavaUtils.convertToJavaArray(listDest)

    for (k <- 1 to k_max) {
      for (l <- 1 to l_max) {
        for (delay <- 1 to delay_max) {
          //          println("ok!")
          //          val calc11 = new KrasKovTE(k,k_tau,l,l_tau,delay).computeContinuous(src,dest)
          calc11.setK(k)
          calc11.setK_tau(k_tau)
          calc11.setL(l)
          calc11.setL_tau(l_tau)
          calc11.setDelay(delay)

          //          println("srcArray ele:"+srcArray.mkString(","))
          //          println("destArray ele:"+srcArray.mkString(","))
          //          result = calc11.computeContinuous(srcArray, destArray)

          array3d(k - 1)(l - 1)(delay - 1) = k + "," + l + "," + delay + "," + calc11.computeContinuous(src, dest, 100)
          //          这个result最后还要由java中的double转成Array中的Double类。
          //          array3d(k - 1)(l - 1)(delay - 1) = String.valueOf(result).toDouble
          //          array3d(k-1)(l-1)(delay-1) = result
          //          println("ok1!")
          //          array3d(k-1)(l-1)(delay-1) = calc11  //此处计算出来的是java.lang.Double,但是存储却用的scala.Double，可能报错
        }
      }
    }
    array3d
  }

  //使用Gaussian计算,ByRDD3
  def computeTwoSeriesTEsStringResultGaussian(src: Array[scala.Double], dest: Array[scala.Double], base: Int, k_max: Int, k_tau: Int, l_max: Int, l_tau: Int, delay_max: Int): Array[String] = {
    var array3d = new ArrayBuffer[String]()
    var calc11 = new GaussianTE()

    import scala.collection.JavaConverters._
    import scala.collection.JavaConversions._
    val listSrc: java.util.List[String] = src.map(x=>x.toString).toBuffer.asJava
    val srcArray = JavaUtils.convertToJavaArray(listSrc)
    val listDest: java.util.List[String] = dest.map(x=>x.toString).toBuffer.asJava
    val destArray = JavaUtils.convertToJavaArray(listDest)

    for (k <- 1 to k_max) {
      for (l <- 1 to l_max) {
        for (delay <- 1 to delay_max) {
            calc11.setParams(k,k_tau,l,l_tau,delay)
//            添加计算出的传递熵信息，字符串，包括：k,l,delay,传递熵值，概率
//            array3d.append(k + "," + l + "," + delay + "," + calc11.computeContinuous(src, dest, 100))
          array3d.append(k + "," + l + "," + delay + "," + calc11.computeContinuous(srcArray, destArray, 100))

        }
      }
    }
    array3d.toArray
  }

  //使用Gaussian计算,ByRDD4
  def computeTwoSeriesTEsStringResultGaussianSrcName(srcName:String, hashMap: HashMap[String,Int], src: Array[scala.Double], dest: Array[scala.Double], base: Int, k_max: Int, k_tau: Int, l_max: Int, l_tau: Int, delay_max: Int): Array[String] = {
    var array3d = new ArrayBuffer[String]()
    var calc11 = new GaussianTE()

    import scala.collection.JavaConverters._
    import scala.collection.JavaConversions._
    val listSrc: java.util.List[String] = src.map(x=>x.toString).toBuffer.asJava
    val srcArray = JavaUtils.convertToJavaArray(listSrc)
    val listDest: java.util.List[String] = dest.map(x=>x.toString).toBuffer.asJava
    val destArray = JavaUtils.convertToJavaArray(listDest)

//    for (k <- 1 to k_max) {
//      for (l <- 1 to l_max) {
//        for (delay <- 1 to delay_max) {
//          calc11.setParams(k,k_tau,l,l_tau,delay)
//          //            添加计算出的传递熵信息，字符串，包括：k,l,delay,传递熵值，概率
//          //            array3d.append(k + "," + l + "," + delay + "," + calc11.computeContinuous(src, dest, 100))
//          array3d.append(k + "," + l + "," + delay + "," + calc11.computeContinuous(srcArray, destArray, 100))
//
//        }
//      }
//    }

    val k_best = hashMap.get(srcName).get

    for(delay <- 1 to delay_max ){
      calc11.setParams(k_best,k_tau,l_max,l_tau,delay)
      array3d.append( k_best + "," + l_max + "," + delay + "," + calc11.computeContinuous(srcArray, destArray, 100) )

    }
    array3d.toArray
  }


//  时序定阶

  def findBestArmaP(ts:Vector,maxP:Int,maxD:Int,maxQ:Int,k_max:Int):Int={

      var best_k = 0
      try {
        best_k = findBestARMAModel(ts,maxP,maxQ,true).p
//        best_k = autoFit(ts.toDense,maxP,maxD,maxQ).p
      }catch {
        case ex:Exception => best_k=k_max
      }

//    autoFit(ts: Vector, maxP: Int = 5, maxD: Int = 2, maxQ: Int = 5)
//    diffedTs: Vector,
//    maxP: Int,
//    maxQ: Int,
//    startWithIntercept: Boolean

    best_k
  }
}


