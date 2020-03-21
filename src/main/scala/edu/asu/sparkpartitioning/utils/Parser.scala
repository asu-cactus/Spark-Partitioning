package edu.asu.sparkpartitioning.utils

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD

object Parser {

  /**
   * Method to read text file containing matrix entries in the
   * format - row,column,value.
   *
   * @param path HDFS path to the text file
   * @param sc [[SparkContext]]
   * @return [[RDD]] of [[MatrixEntry]]
   */
  def readMatrix(path: String)(implicit sc: SparkContext): RDD[MatrixEntry] = {
    val textRDD = sc.textFile(path)
    val rddString: RDD[Array[String]] = textRDD.map(_.split(',').map(_.trim))
    rddString.map({
      case Array(row, col, value) =>
        MatrixEntry(row.toLong, col.toLong, value.toDouble)
    })
  }
}
