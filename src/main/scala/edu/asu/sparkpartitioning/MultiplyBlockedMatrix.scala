package edu.asu.sparkpartitioning
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.mllib.linalg.distributed.BlockMatrix

object MultiplyBlockedMatrix {

  def main(args: Array[String]): Unit = {

    if (args.length != 8) {
      throw new IllegalArgumentException(
        "Usage: rowsPerBlock, colsPerBlock, nLeftRows, nLeftCols, " +
          "nRightRows, nRightCols, inputLeftFilePath inputRightFilePath"
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

    var useBreeze: Boolean = true

    val conf = new SparkConf()
      .setAppName("MultiplyBlockedMatrix")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    if (!useBreeze) {
      val left_blocks = sc
        .objectFile[((Int, Int), org.apache.spark.mllib.linalg.Matrix)](
          inputLeftFilePath
        )
        .cache()
      val right_blocks = sc
        .objectFile[((Int, Int), org.apache.spark.mllib.linalg.Matrix)](
          inputRightFilePath
        )
        .cache()
      //run some MLlib linear algebra tests
      val blocked_left_matrix =
        new BlockMatrix(
          left_blocks,
          rowsPerBlock,
          colsPerBlock,
          nLeftRows,
          nLeftCols
        ).cache()

      val blocked_right_matrix =
        new BlockMatrix(
          right_blocks,
          rowsPerBlock,
          colsPerBlock,
          nRightRows,
          nRightCols
        ).cache()

      val result = blocked_left_matrix.multiply(blocked_right_matrix)
      println("count of blocks:" + result.blocks.count())
      println("numRowBlocks:" + (result.numRowBlocks))
      println("numColBlocks:" + (result.numColBlocks))
    } else {

      //customized breeze-based matrix multiplication of two distributed matrices

      val left_blocks = sc
        .objectFile[((Int, Int), breeze.linalg.DenseMatrix[Double])](
          inputLeftFilePath
        )
        .cache
      val right_blocks = sc
        .objectFile[((Int, Int), breeze.linalg.DenseMatrix[Double])](
          inputRightFilePath
        )
        .cache

      val lblocks = left_blocks.map({
        case block => (block._1._2, (block._1._1, block))
      })
      val rblocks = right_blocks.map({
        case block => (block._1._1, (block._1._2, block))
      })

      val P: org.apache.spark.rdd.RDD[
        ((Int, Int), breeze.linalg.DenseMatrix[Double])
      ] = lblocks
        .join(rblocks)
        .map({
          case (_, ((r, lb), (c, rb))) =>
            val B: breeze.linalg.DenseMatrix[Double] = rb._2 match {
              case dense: breeze.linalg.DenseMatrix[Double] => lb._2 * dense
              case _ =>
                throw new SparkException(
                  s"Unrecognized matrix type ${rb._2.getClass}."
                )
            }
            ((r, c), B)
        })
        .reduceByKey((a, b) => a + b)
      println("count of blocks:" + P.count())
    }
  }

}
