package edu.asu.sqlbucketing

import edu.asu.sqlbucketing.experiments._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object Main {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      throw new IllegalArgumentException(
        "Base path for storing data, experiment " +
          "to execute and number of partitions is expected." +
          s"\nProvide: ${args.toList}"
      )
    }
    val basePath = args(0)
    val numOfParts = args(1).toInt
    val experiment = args(2)

    implicit val log: Logger = Logger.getLogger("MatrixMultiplication")

    val conf = new SparkConf().setAppName(s"bucket_multiplication_$experiment")

    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .getOrCreate()

    experiment match {
      case "e1" => new E1(numOfParts).execute(basePath)
      case "e2" => new E2(numOfParts).execute(basePath)
    }

    spark.sql("DROP TABLE left")
    spark.sql("DROP TABLE right")
  }

}
