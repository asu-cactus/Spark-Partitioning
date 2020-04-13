package edu.asu.parquetfiles.utils

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
      key.asInstanceOf[Int] % numberOfParts
  }

  /**
   * Partitioner for Left Block Matrix
   */
  class LeftPartitioner(
    val rows: Int,
    val cols: Int,
    val rowsPerBlock: Int,
    val colsPerBlock: Int
  ) extends Partitioner {

    require(rows % rowsPerBlock == 0)
    require(cols % colsPerBlock == 0)

    override val numPartitions: Int =
      (rows / rowsPerBlock) * (cols / colsPerBlock)

    /**
     * Returns the index of the partition the block belongs to
     * @param key the <block_row_id, block_col_id> pair
     * @return the index of the partition
     */
    override def getPartition(key: Any): Int =
      key match {
        case (i: Int, j: Int)         => j
        case (i: Int, j: Int, _: Int) => j
        case _ =>
          throw new IllegalArgumentException(s"Unrecognized key: $key.")
      }

    override def equals(obj: Any): Boolean =
      obj match {
        case r: LeftPartitioner =>
          (this.rows == r.rows) && (this.cols == r.cols) &&
            (this.rowsPerBlock == r.rowsPerBlock) && (this.colsPerBlock == r.colsPerBlock)
        case _ => false

      }

    override def hashCode: Int =
      com.google.common.base.Objects.hashCode(
        rows: java.lang.Integer,
        cols: java.lang.Integer,
        rowsPerBlock: java.lang.Integer,
        colsPerBlock: java.lang.Integer
      )
  }

  /**
   * Partitioner for Right Block Matrix
   */
  class RightPartitioner(
    rows: Int,
    cols: Int,
    rowsPerBlock: Int,
    colsPerBlock: Int
  ) extends LeftPartitioner(rows, cols, rowsPerBlock, colsPerBlock) {

    /**
     * Returns the index of the partition the block belongs to
     * @param key the <block_row_id, block_col_id> pair
     * @return the index of the partition
     */
    override def getPartition(key: Any): Int =
      key match {
        case (i: Int, j: Int)         => i
        case (i: Int, j: Int, _: Int) => i
        case _ =>
          throw new IllegalArgumentException(s"Unrecognized key: $key.")
      }

  }

}
