package io.github.pratikbarhate.sparklingmatrixmultiplication.utils

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD

object Parser {
  def readMatrix(path: String)(implicit sc: SparkContext): RDD[MatrixEntry] = {
    val textRDD = sc.textFile(path)
    val rddString: RDD[Array[String]] = textRDD.map(_.split(',').map(_.trim))
    rddString.map({ case Array(row, col, value) => MatrixEntry(row.toLong, col.toLong, value.toDouble) })
  }
}
