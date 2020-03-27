package edu.asu.sparkpartitioning.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Parser {

  /**
   * Method to read text file containing matrix entries in the
   * format - row,column,value.
   *
   * @param path HDFS path to the text file
   * @param sc [[SparkContext]] the entry point to the app
   * @return [[RDD]] of [[MatrixEntry]]
   */
  def readMatrix(path: String)(implicit sc: SparkContext): RDD[MatrixEntry] = {
    val textRDD = sc.textFile(path)
    val rddString: RDD[Array[String]] = textRDD.map(_.split(',').map(_.trim))
    rddString.map({
      case Array(row, col, value) =>
        MatrixEntry(row.toInt, col.toInt, value.toDouble)
    })
  }

  /**
   *  Custom matrix entry case class
   *
   * @param i Row ID of the matrix
   * @param j Column ID of the matrix
   * @param value Cell Value
   */
  case class MatrixEntry(i: Int, j: Int, value: Double) extends Serializable
}
