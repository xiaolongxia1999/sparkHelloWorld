package commonUtils


import com.cloudera.sparkts.models._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.DenseVector

import scala.collection.mutable.ArrayBuffer
import java.io.File
import java.nio.file.Files
import java.sql.Timestamp
import java.time._

import org.apache.spark.mllib.linalg.{DenseVector, Vector}

import scala.Double.NaN
import com.cloudera.sparkts.DateTimeIndex._
import com.cloudera.sparkts._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.scalatest.{FunSuite, ShouldMatchers}

import scala.collection.Map

//import org.apache.spark.mllib.linalg.DenseVector
//import org.apache.spark.mllib.linalg.Vector
/**
  * Created by Administrator on 2018/5/24 0024.
  */
object test {

  def findBestArimaModel(series:Vector,maxP:Int,maxD:Int,maxQ:Int):ARIMAModel={
      return ARIMA.autoFit(series,maxP,maxD,maxQ)
  }

  def findBBestArimaModel(series:Array[Double],maxP:Int,maxD:Int,maxQ:Int):ARIMAModel={
    return ARIMA.autoFit(new DenseVector(series),maxP,maxD,maxQ)
  }



  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf().setMaster("local[*]").setAppName("ts")
//    val sc = new SparkContext(conf)
//    val ts = new DenseVector(Array(1.0,2.0,4.0,16.0,256.0,60023,360000000))
//val ts = new DenseVector(Array(1.0,2.0,3.0,5.0,2.0,1.0,2.1,2,3,4,5,6,7,4,3,2,5,1,3,4,1,4,1,4,6,7,3,5))
    val ab = new ArrayBuffer[Double]()
    for(i<-0 to 40){
      ab.append(new util.Random().nextInt(50).toDouble)
    }
    print("array is:"+ab.mkString("\t"))
    val ts = new DenseVector(ab.toArray)
    val maxP = 20
    val maxD = 2
    val maxQ = 20
//    val arima = ARIMA.autoFit(ts,maxP,maxD,maxQ)

    val arima = this.findBestArimaModel(ts,maxP,maxD,maxQ)
    println("is stationary:"+arima.isStationary())
    println("AIC value:"+arima.approxAIC(ts))
    print(s"best model：p is:${arima.p},q is:${arima.d},q is ${arima.q}")


//    val dataFile = getClass.getClassLoader.getResourceAsStream("R_ARIMA_DataSet1.csv")


    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    TimeSeriesKryoRegistrator.registerKryoClasses(conf)
    val sc = new SparkContext(conf)
    val vecs = Array(0 until 10, 10 until 20, 20 until 30)
      .map(_.map(x => x.toDouble).toArray)
      .map(new DenseVector(_))
      .map(x => (x(0).toString, x))
    val start = ZonedDateTime.of(2015, 4, 9, 0, 0, 0, 0, ZoneId.of("Z"))
    val index = uniform(start, 10, new DayFrequency(1))         //这个应该把索引、rdd的长度，进行对齐
    val rdd = new TimeSeriesRDD[String](index, sc.parallelize(vecs))
    val slice = rdd.slice(start.plusDays(1), start.plusDays(6))
//    slice.index should be (uniform(start.plusDays(1), 6, new DayFrequency(1)))
//    val contents = slice.collectAsMap()
//    contents.size should be (3)
//    contents("0.0") should be (new DenseVector((1 until 7).map(_.toDouble).toArray))
//    contents("10.0") should be (new DenseVector((11 until 17).map(_.toDouble).toArray))
//    contents("20.0") should be (new DenseVector((21 until 27).map(_.toDouble).toArray))
    println(rdd.collect().mkString("@"))
    print("hh:"+rdd.keys.mkString("@"))

    val zone = ZoneId.systemDefault()
    var dtIndex:UniformDateTimeIndex=DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, zone),
      ZonedDateTime.of(2017, 5, 31, 23, 55, 0, 0, zone),
      new MinuteFrequency(5)
    )

    print(dtIndex.toNanosArray())

  }
}
