package edu.asu.sparkpartitioning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import edu.asu.sparkpartitioning.utils.MatrixPartitioners._
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

object PartitionBlockedMatrix {

  def main(args: Array[String]): Unit = {

    if (args.length < 6) {
      throw new IllegalArgumentException(
        "Usage: numRows numCols rowsPerBlock colsPerBlock inputFilePath outputFilePath r(optional: to specify right partitioning. By default it is left partitioning.)"
      )
    }
    val numRows = args(0).toInt
    val numCols = args(1).toInt
    val rowsPerBlock = args(2).toInt
    val colsPerBlock = args(3).toInt
    val inputFilePath = args(4)
    val outputFilePath = args(5)
    
    var left: Boolean = true
    
    if (args.length == 7) {
        if (args(6) == 'r') {
            left = false
        }
    }

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

    var partitioner = new LeftPartitioner(numRows, numCols, rowsPerBlock, colsPerBlock)

    //create a left partitioner
    if (!left) {
        partitioner = new RightPartitioner(numRows, numCols, rowsPerBlock, colsPerBlock)
    } 

    //partition the data
    val partitioned_blocks = parsed_blocks.partitionBy(partitioner).cache()
    
    //write the RDD of Blocks to an object file
    partitioned_blocks.saveAsObjectFile(outputFilePath)

  }

}
