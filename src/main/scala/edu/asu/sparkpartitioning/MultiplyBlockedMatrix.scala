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

    if (args.length != 8) {
      throw new IllegalArgumentException(
        "Usage: rowsPerBlock, colsPerBlock, nLeftRows, nLeftCols, nRightRows, nRightCols, inputLeftFilePath inputRightFilePath"
      )
    }
    val rowsPerBlock = args(0).toInt
    val colsPerBlock = args(1).toInt
    val nLeftRows = args(2).toInt
    val nLeftCols = args(3).toInt
    val nRightRows = args(4).toInt
    val nRightCols = args(5).toInt
    val inputLeftFilePath = args(6)
    val inputRightFilePath = args(7)

    val conf = new SparkConf()
      .setAppName("MultiplyBlockedMatrix")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val left_blocks = sc.objectFile[((Int, Int), org.apache.spark.mllib.linalg.Matrix)](inputLeftFilePath).cache()
    val right_blocks = sc.objectFile[((Int, Int), org.apache.spark.mllib.linalg.Matrix)](inputRightFilePath).cache()

    //run some MLlib linear algebra tests
    val blocked_left_matrix =
      new BlockMatrix(left_blocks, rowsPerBlock, colsPerBlock, nLeftRows, nLeftCols).cache()

    val blocked_right_matrix =
      new BlockMatrix(right_blocks, rowsPerBlock, colsPerBlock, nRightRows, nRightCols).cache()

    val result = blocked_left_matrix.multiply(blocked_right_matrix)
    println("numRowBlocks:"+(result.numRowBlocks))
    println("numColBlocks:"+(result.numColBlocks))
  }

}
