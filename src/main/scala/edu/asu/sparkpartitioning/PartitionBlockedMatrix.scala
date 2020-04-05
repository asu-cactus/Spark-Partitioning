package edu.asu.sparkpartitioning

import edu.asu.sparkpartitioning.utils.MatrixPartitioners._
import org.apache.spark.{SparkConf, SparkContext}

object PartitionBlockedMatrix {

  def main(args: Array[String]): Unit = {

    if (args.length < 6) {
      throw new IllegalArgumentException(
        "Usage: numRows numCols rowsPerBlock colsPerBlock inputFilePath " +
          "outputFilePath r(optional: to specify right partitioning. " +
          "By default it is left partitioning.)"
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
      if (args(6) == "r") {
        left = false
      }
    }

    var useBreeze: Boolean = true

    val conf = new SparkConf()
      .setAppName("PartitionBlockedMatrix")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val data = sc.textFile(inputFilePath)

    //transform the data into an RDD of blocks
    val parsed_blocks = data
      .map { line =>
        val tokens = line.split(' ')

        if (useBreeze) {

          //if use breeze, we randomly create a block instead of using the block from the input file
          //this is fine for performance test

          (
            (tokens(0).toInt, tokens(1).toInt),
            breeze.linalg.DenseMatrix.rand[Double](
              rowsPerBlock,
              colsPerBlock
            )
          )
        } else {

          // if use spark DenseMatrix, we read from the file

          (
            (tokens(0).toInt, tokens(1).toInt),
            org.apache.spark.mllib.linalg.Matrices.dense(
              rowsPerBlock,
              colsPerBlock,
              tokens.tail.tail.map(_.toDouble)
            )
          )
        }
      }
      .cache()

    var partitioner =
      new LeftPartitioner(numRows, numCols, rowsPerBlock, colsPerBlock)

    //create a left partitioner
    if (!left) {
      partitioner =
        new RightPartitioner(numRows, numCols, rowsPerBlock, colsPerBlock)
    }

    //partition the data
    val partitioned_blocks = parsed_blocks.partitionBy(partitioner).cache()

    //write the RDD of Blocks to an object file
    partitioned_blocks.saveAsObjectFile(outputFilePath)

  }

}
