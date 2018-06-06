package spark.streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.examples.streaming.StreamingExamples
/**
  * Counts words in new text files created in the given directory
  * Usage: HdfsWordCount <directory>
  *   <directory> is the directory that Spark Streaming will use to find and read new text files.
  *
  * To run this on your local machine on directory `localdir`, run this example
  *    $ bin/run-example \
  *       org.apache.spark.examples.streaming.HdfsWordCount localdir
  *
  * Then create a text file in `localdir` and the words in the file will get counted.
  */
object hdfsStreaming {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }
    println("hello")
//    StreamingExamples.setStreamingLogLevels()
//    val sparkConf = new SparkConf().setMaster("spark://192.16.32.139:7077").setAppName("HdfsWordCount")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HdfsWordCount")
    // Create the context
    println("begin")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    println("begin1")
    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(args(0))
    println("begin1.5")
    val words = lines.flatMap(_.split(" "))
    println("begin1.7")
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    println("begin2")
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
