package edu.asu.parquetfiles.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Parser {

  /**
   * Method to read text file containing matrix entries in the
   * format - row,column,value.
   *
   * @param path HDFS path to the text file
   * @param ss [[SparkSession]] the entry point to the app
   * @return [[RDD]] of [[MatrixEntry]]
   */
  def readMatrix(path: String)(implicit ss: SparkSession): DataFrame = {

    import ss.implicits._
    val textRDD = ss.sparkContext.textFile(path)
    val mappedRDD = textRDD.map(x => x.split(",")).map {
      case Array(row, col, value) =>
        MatrixEntry(row.toInt, col.toInt, value.toDouble)
    }
    mappedRDD.toDF()

  }

  /**
   *  Custom matrix entry case class
   * @param i Row ID of the matrix
   * @param j Column ID of the matrix
   * @param value Cell Value
   */
  case class MatrixEntry(i: Int, j: Int, value: Double) extends Serializable
}
