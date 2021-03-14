package edu.asu.linearalgebra

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      throw new IllegalArgumentException(
        "Base path for storing data and  type of execution " +
          "is expected." +
          s"\nProvide: ${args.toList}"
      )
    }

    val basePath = args(0)
    val experimentType = args(1)
    val blockRow = args(2).toInt
    val blockCol = args(3).toInt

    implicit val log: Logger = Logger.getLogger("LinearAlgebraTest")

    val conf = new SparkConf()
      .setAppName(s"algebra_multiplication_$experimentType")

    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .getOrCreate()

    new Plain(blockRow, blockCol).execute(basePath)
  }

}
