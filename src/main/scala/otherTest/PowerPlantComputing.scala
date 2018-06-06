package otherTest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

import org.apache.spark.sql.functions._
/**
  * Created by Administrator on 2018/4/3 0003.
  */
object PowerPlantComputing {
  def main(args: Array[String]): Unit = {

    val start = System.currentTimeMillis();
    val spark = SparkSession.builder().master("local[*]").appName("pp_computing").config("park.sql.pivotMaxValues","20000").getOrCreate();
//    val inputPath ="F:/BigData/000000_1_copy_1";
    val inputPath ="F:/BigData/00000*";
    val outputPath = "F:/BigData/out"

    var df_source = spark.read.csv(inputPath);

//    数组里的切割,在scala中用()而不是[]
//    df_source = df_source.withColumn("_c3", col("_c3").cast(DoubleType)).withColumn("_c4",split(col("_c4"),":")(0))
//                      .groupBy(col("_c0"),col("_c4")).avg("_c3").groupBy("_c4").pivot("_c0").avg("avg(_c3)")
//                    .select(col("_c0"),col("_c4"),col("_c3"));

    df_source = df_source.withColumn("_c3", col("_c3").cast(DoubleType)).withColumn("_c4",split(col("_c4"),":")(0))
                          .groupBy(col("_c0"),col("_c4")).avg("_c3").groupBy("_c4").pivot("_c0").avg("avg(_c3)");

//    df_source.printSchema();
//    df_source.show(20);

    df_source.write.option("header",true).mode("overwrite").csv(outputPath);

    val delta = System.currentTimeMillis()-start;
    println(("consumed time is:" + delta / 1000))
  }
}
