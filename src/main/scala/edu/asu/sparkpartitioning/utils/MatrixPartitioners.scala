package edu.asu.sparkpartitioning.utils

import org.apache.spark.Partitioner

object MatrixPartitioners {

  /**
   * Partitioner for the matrix multiplication operation.
   *
   * @param numberOfParts Number of partitions to create
   */
  class IndexedPartitioner(numberOfParts: Int) extends Partitioner {
    override def numPartitions: Int = numberOfParts

    override def getPartition(key: Any): Int =
      (key.asInstanceOf[Long] % numberOfParts.toLong).toInt
  }

}
