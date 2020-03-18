package io.github.pratikbarhate.sparklingmatrixmultiplication.utils

import org.apache.spark.Partitioner

object MatrixPartitioners {

  class IndexedPartitioner(numberOfParts: Int) extends Partitioner {
    override def numPartitions: Int = numberOfParts

    override def getPartition(key: Any): Int = (key.asInstanceOf[Long] % numberOfParts.toLong).toInt
  }

}
