package edu.asu.sparkpartitioning

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Matrices

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

    var useBreeze: Boolean = true
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
          (
            (tokens(0).toInt, tokens(1).toInt),
            Matrices.dense(
              rowsPerBlock,
              colsPerBlock,
              tokens.tail.tail.map(_.toDouble)
            )
          )
        }
      }
      .cache()

    //write the RDD of Blocks to an object file
    parsed_blocks.saveAsObjectFile(outputFilePath)

    parsed_blocks.keys.foreach(println)

  }

}
