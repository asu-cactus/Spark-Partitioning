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

object ParseBlockedMatrix {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      throw new IllegalArgumentException(
        "Usage: rowsPerBlock colsPerBlock inputFilePath outputFilePath"
      )
    }
    val rowsPerBlock = args(0).toInt
    val colsPerBlock = args(1).toInt
    val inputFilePath = args(2)
    val outputFilePath = args(3)

    val conf = new SparkConf()
      .setAppName("ParseBlockedMatrix")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val data = sc.textFile(inputFilePath)

    //transform the data into an RDD of blocks
    val parsed_blocks = data
      .map { line =>
        val tokens = line.split(' ')
        (
          (tokens(0).toInt, tokens(1).toInt),
          Matrices.dense(
            rowsPerBlock,
            colsPerBlock,
            tokens.tail.tail.map(_.toDouble)
          )
        )
      }
      .cache()

    //write the RDD of Blocks to an object file
    parsed_blocks.saveAsObjectFile(outputFilePath)

    //run some MLlib linear algebra tests
    val blocked_matrix =
      new BlockMatrix(parsed_blocks, rowsPerBlock, colsPerBlock)
    blocked_matrix.cache()
    val result = blocked_matrix.multiply(blocked_matrix.transpose)

  }

}
