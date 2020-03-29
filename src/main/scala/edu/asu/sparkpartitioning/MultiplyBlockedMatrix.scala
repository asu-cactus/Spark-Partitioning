package edu.asu.sparkpartitioning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{
  DenseMatrix,
  DenseVector,
  Matrices,
  Matrix,
  Vector,
  Vectors
}
import org.apache.spark.mllib.linalg.distributed.{
  BlockMatrix,
  CoordinateMatrix,
  IndexedRow,
  IndexedRowMatrix,
  MatrixEntry,
  RowMatrix
}

object MultiplyBlockedMatrix {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      throw new IllegalArgumentException(
        "Usage: rowsPerBlock, colsPerBlock, inputLeftFilePath inputRightFilePath"
      )
    }
    val rowsPerBlock = args(0).toInt
    val colsPerBlock = args(1).toInt
    val inputLeftFilePath = args(2)
    val inputRightFilePath = args(3)

    val conf = new SparkConf()
      .setAppName("ParseBlockedMatrix")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val left_blocks = sc.objectFile[((Int, Int), org.apache.spark.mllib.linalg.Matrix)](inputLeftFilePath).cache()
    val right_blocks = sc.objectFile[((Int, Int), org.apache.spark.mllib.linalg.Matrix)](inputRightFilePath).cache()

    //run some MLlib linear algebra tests
    val blocked_left_matrix =
      new BlockMatrix(left_blocks, rowsPerBlock, colsPerBlock).cache()

    val blocked_right_matrix =
      new BlockMatrix(right_blocks, rowsPerBlock, colsPerBlock).cache()

    val result = blocked_left_matrix.multiply(blocked_right_matrix)
    println("numRowBlocks:"+(result.numRowBlocks))
    println("numColBlocks:"+(result.numColBlocks))
  }

}
