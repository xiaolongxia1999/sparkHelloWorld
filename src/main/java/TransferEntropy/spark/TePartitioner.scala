package TransferEntropy.spark

import org.apache.spark.Partitioner

/**
  * Created by Administrator on 2018/4/26 0026.
  */
class TePartitioner(partitionNum:Int) extends Partitioner{
  override def numPartitions: Int = partitionNum

  //此处的key必须为String，String包含数据所在的行、列信息即可
  override def getPartition(key: Any): Int = {
    val location = key.toString
    val partitioner = location.hashCode % numPartitions
    if (partitioner<0){
      partitioner + numPartitions
    }else{
      partitioner
    }
  }

}
