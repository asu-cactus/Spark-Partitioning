package edu.asu.linearalgebra

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix

class Plain()(implicit spark: SparkSession) {

  def execute(basePath: String)(): Unit = {

    val leftDF = spark.read.parquet(s"$basePath/common/left")
    val rightDF = spark.read.parquet(s"$basePath/common/right")

    val leftRow: RDD[MatrixEntry] = leftDF.rdd
      .map(x =>
        MatrixEntry(
          x.getAs[Int]("rowID").toLong,
          x.getAs[Int]("columnID").toLong,
          x.getAs[Double]("value")
        )
      )

    val rightRow: RDD[MatrixEntry] = rightDF.rdd
      .map(x =>
        MatrixEntry(
          x.getAs[Int]("rowID").toLong,
          x.getAs[Int]("columnID").toLong,
          x.getAs[Double]("value")
        )
      )

    val leftMat = new CoordinateMatrix(leftRow).toBlockMatrix.cache
    val rightMat = new CoordinateMatrix(rightRow).toBlockMatrix.cache

    leftMat.cache()
    rightMat.cache()
    // To trigger the caching
    leftMat.blocks.count()
    rightMat.blocks.count()
    // Transformation stage for multiplication operation
    val res = leftMat.multiply(rightMat)
    // To trigger the multiplication operation
    res.blocks.count()
  }

}
