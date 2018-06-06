package mllib.chapter1

import breeze.linalg
import breeze.linalg.{DenseMatrix, DenseVector, diag}
import breeze.optimize.proximal.LinearGenerator
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.util.{KMeansDataGenerator, LinearDataGenerator, LogisticRegressionDataGenerator, SVMDataGenerator}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/3/22 0022.
  */
class page17 {

}
object page17{
  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("page17")
    val sc = new SparkContext(sparkConf)

    sc.setLogLevel(logLevel = "ERROR")
    val data_path = "F:\\sparkHelloWorld\\sparkMLlib\\data\\data.txt"
    val data = sc.textFile(data_path).map(_.split(" ")).map(f=>f.map(f=>f.toDouble))
    val data1 = data.map(f=>Vectors.dense(f))
    val stat1 = Statistics.colStats(data1)
//    println(stat1.max)
//    println(stat1.min)
//
//    println(stat1.variance)
//
//    val corr1 = Statistics.corr(data1,"pearson")
//    val corr2 = Statistics.corr(data1,"spearman")
//    val x1 = sc.parallelize(Array(1.0,1.0,3.0,4.0))
//    val x2 = sc.parallelize(Array(5.0,6.0,6.0,6.0))
//
//    val corr3 = println(Statistics.corr(x1, x2, "pearson"))

//KMeans样本生成
//    val KMeansRDD = KMeansDataGenerator.generateKMeansRDD(sc,40,5,3,1.0,2)
//    println(KMeansRDD.count())
//    KMeansRDD.take(5)

//    线性回归样本生成
//    val linearRDD = LinearDataGenerator.generateLinearRDD(sc,40,3,1.0,2,0.0)
//    println(linearRDD.count())
//    linearRDD.take(5).foreach(println)

//    逻辑回归RDD生成
//    val LogisticRDD =LogisticRegressionDataGenerator.generateLogisticRDD(sc,40,3,1.0,2,0.5)
//    println(LogisticRDD.count())
//    LogisticRDD.take(5).foreach(println)

    //breeze
//    val m1 = DenseMatrix.zeros[Double](2,3)
//    val v1 = DenseVector.zeros[Double](3)
//    val v2 = DenseVector.ones[Double](3)
//    val v3 = DenseVector.fill(3){5.0}
//    val m2 = DenseMatrix.eye[Double](3)
//    val v6 = diag(DenseVector(1.0,2.0,3.0))
//
//    println(m1)
//    println(v1)
//    println(v2)
//    println(v3)
//    println(m2)
//    println(v6)

    val m3 = DenseMatrix((1.0,2.0),(3.0,4.0))
    val v8 = DenseVector(1,2,3,4)
    val v9 = v8.t
    println(m3)
    println(v8)
    println(v9)
    println("hh")
    val v10 = DenseVector.tabulate(3)(i=>2*i)
    val m4 = DenseMatrix.tabulate(3,2){case (i,j)=>i+j}
    val v11 = new DenseVector(Array(1,2,3,4))
    println(v10)
    println (m4)
    println(v11)
    println("heheh")
    val m5 = new DenseMatrix(2,3,Array(11,12,13,14,15,16))
    val v12 = DenseVector.rand(4)
    val m6 = DenseMatrix.rand(2,3)
    println(m5)
    println(v12)
    println(m6)





  }
}
