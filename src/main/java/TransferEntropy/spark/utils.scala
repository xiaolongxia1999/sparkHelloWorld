package TransferEntropy.spark

import java.util

import TransferEntropy.JavaUtils.JavaUtils
import TransferEntropy.spark.servicesImpl.{GaussianTE, KrasKovTE}

import scala.collection.mutable.ArrayBuffer
import java.lang._
import java.util._

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
/**
  * Created by Administrator on 2018/4/26 0026.
  */
object utils {
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
          arrayBuffer.append("" + key + "," +arr(k)(l)(delay)) //+System.lineSeparator())，展示不需要换行符
        }
      }
    }
    arrayBuffer.toArray
  }

  //  TE的计算结果，将（key,Array[][][])转成RDD[String] ,其中String为以lineSeperator分隔的多行,每行格式： key,k,l,delay,value
  //貌似array3dToString已经实现了，按行分隔
  //  def array3dToLines(key:String,arr:Array[Array[Array[Double]]]):Arra


  //  这是原始的版本，引入java.lang._无法通过编译，改成下面的同样的方法
  //  def computeTwoSeriesTEs(src:Array[Double],dest:Array[Double],base:Int,k_max:Int,k_tau:Int,l_max:Int,l_tau:Int,delay_max:Int):Array[Array[Array[Double]]] ={
  //    var array3d = Array.ofDim[Double](k_max,l_max,delay_max)
  //    var calc11 = new KrasKovTE()
  //    var result = 0.0
  //    for(k<-1 to k_max){
  //      for(l<-1 to l_max){
  //        for(delay<-1 to delay_max){
  //          //          println("ok!")
  //          //          val calc11 = new KrasKovTE(k,k_tau,l,l_tau,delay).computeContinuous(src,dest)
  //          calc11.setK(k)
  //          calc11.setK_tau(k_tau)
  //          calc11.setL(l)
  //          calc11.setL_tau(l_tau)
  //          calc11.setDelay(delay)
  //          //这里src和dest都要由scala的Array[Double]转成double
  //          result = calc11 computeContinuous(src, dest)
  //          //          这个result最后还要由java中的double转成Array中的Double类。
  //
  //          array3d(k-1)(l-1)(delay-1) = result
  //          //          println("ok1!")
  //          //          array3d(k-1)(l-1)(delay-1) = calc11  //此处计算出来的是java.lang.Double,但是存储却用的scala.Double，可能报错
  //        }
  //      }
  //    }
  //    array3d
  //  }
  //
  //




  //计算连续情形
  def computeTwoSeriesTEs(src: Array[scala.Double], dest: Array[scala.Double], base: Int, k_max: Int, k_tau: Int, l_max: Int, l_tau: Int, delay_max: Int): Array[Array[Array[scala.Double]]] = {
    var array3d = Array.ofDim[scala.Double](k_max, l_max, delay_max)
    var calc11 = new KrasKovTE()
    var result = 0.0
    //scala的ArrayBuffer转java的List,List再转double[]
    //这里src和dest都要由scala的Array[Double]转成double
    import scala.collection.JavaConverters._
    import scala.collection.JavaConversions._
    val listSrc: java.util.List[String] = src.map(x=>x.toString).toBuffer.asJava
    val srcArray = JavaUtils.convertToJavaArray(listSrc)
    val listDest: java.util.List[String] = dest.map(x=>x.toString).toBuffer.asJava
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
    val listSrc: java.util.List[String] = src.map(x=>x.toString).toBuffer.asJava
    val srcArray = JavaUtils.convertToJavaArray(listSrc)
    val listDest: java.util.List[String] = dest.map(x=>x.toString).toBuffer.asJava
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

          array3d(k-1)(l-1)(delay-1) = k+","+l+","+delay+","+calc11.computeContinuous(src,dest,100)
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

  //使用Gaussian计算
  def computeTwoSeriesTEsStringResultGaussian(src: Array[scala.Double], dest: Array[scala.Double], base: Int, k_max: Int, k_tau: Int, l_max: Int, l_tau: Int, delay_max: Int): Array[Array[Array[String]]] = {
    var array3d = Array.ofDim[String](k_max, l_max, delay_max)
    var calc11 = new GaussianTE()
    //    var result = 0.0
    //scala的ArrayBuffer转java的List,List再转double[]
    //这里src和dest都要由scala的Array[Double]转成double
    import scala.collection.JavaConverters._
    import scala.collection.JavaConversions._
    val listSrc: java.util.List[String] = src.map(x=>x.toString).toBuffer.asJava
    val srcArray = JavaUtils.convertToJavaArray(listSrc)
    val listDest: java.util.List[String] = dest.map(x=>x.toString).toBuffer.asJava
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

          array3d(k-1)(l-1)(delay-1) = k+","+l+","+delay+","+calc11.computeContinuous(src,dest,100)
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




//  new太多对象，会导致垃圾回收过多，弃用
//
//  def computeTwoSeriesTEs(src:Array[Double],dest:Array[Double],base:Int,k_max:Int,k_tau:Int,l_max:Int,l_tau:Int,delay_max:Int):Array[Array[Array[Double]]] ={
//    var array3d = Array.ofDim[Double](k_max,l_max,delay_max)
//    for(k<-1 to k_max){
//      for(l<-1 to l_max){
//        for(delay<-1 to delay_max){
//          //          println("ok!")
//          val calc11 = new KrasKovTE(k,k_tau,l,l_tau,delay).computeContinuous(src,dest)
//          //          println("ok1!")
//          array3d(k-1)(l-1)(delay-1) = calc11  //此处计算出来的是java.lang.Double,但是存储却用的scala.Double，可能报错
//        }
//      }
//    }
//    array3d
//  }


//  def main(args: Array[String]): Unit = {
//    val src1= Array(1.0,2.0,3.0)
//    val listSrc: java.util.List[String] = src1.map(x=>x.toString).toBuffer.asJava
//    val srcArray = JavaUtils.convertToJavaArray(listSrc)
//    //    println("x")
//    for(i<-srcArray){
//      println(i)
//    }
//  }
}
