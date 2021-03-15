package edu.asu.linearalgebra

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      throw new IllegalArgumentException(
        "Base path for storing data and  type of execution " +
          "is expected." +
          s"\nProvide: ${args.toList}"
      )
    }

    val basePath = args(0)
    val experimentType = args(1)

    implicit val log: Logger = Logger.getLogger("LinearAlgebraTest")

    val conf = new SparkConf()
      .setAppName(s"algebra_multiplication_$experimentType")

    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .getOrCreate()

    new Plain().execute(basePath)
  }

}
