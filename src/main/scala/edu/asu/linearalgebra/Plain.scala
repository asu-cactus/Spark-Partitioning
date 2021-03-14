package edu.asu.linearalgebra

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.max
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix

class Plain(bRow: Int, bCol: Int)(implicit spark: SparkSession) {

  def execute(basePath: String)(): Unit = {

    val leftDF = spark.read.parquet(s"$basePath/common/left")
    val rightDF = spark.read.parquet(s"$basePath/common/right")

    val maxLeft = leftDF
      .select(max("columnID"))
      .first()
      .getInt(0)
    val maxRight = rightDF
      .select(max("columnID"))
      .first()
      .getInt(0)
    val colSize = maxLeft.max(maxRight)

    val leftIndexedRow: RDD[IndexedRow] = leftDF.rdd
      .groupBy(g => g.getAs[Int]("rowID"))
      .map { m: (Int, Iterable[Row]) =>
        val indices = m._2.map(i => i.getAs[Int]("columnID"))
        val values = m._2.map(i => i.getAs[Double]("value"))
        val vec = new SparseVector(colSize, indices.toArray, values.toArray)
        IndexedRow(m._1, vec)
      }

    val rightIndexedRow: RDD[IndexedRow] = rightDF.rdd
      .groupBy(g => g.getAs("rowID"))
      .map { m: (Int, Iterable[Row]) =>
        val indices = m._2.map(i => i.getAs[Int]("columnID"))
        val values = m._2.map(i => i.getAs[Double]("value"))
        val vec = new SparseVector(colSize, indices.toArray, values.toArray)
        IndexedRow(m._1, vec)
      }

    val leftMat = new IndexedRowMatrix(leftIndexedRow)
      .toBlockMatrix(bRow, bCol)
      .cache()
    val rightMat = new IndexedRowMatrix(rightIndexedRow)
      .toBlockMatrix(bCol, bRow)
      .cache()

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
