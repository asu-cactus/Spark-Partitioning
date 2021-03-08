package edu.asu.sqlpartitioning

import edu.asu.sqlpartitioning.utils.Parser.readMatrix
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object TextToParquetFiles {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      throw new IllegalArgumentException(
        "Base path for storing data and number of parts are expected." +
          s"\nProvide: ${args.toList}"
      )
    }

    val basePath = args(0)
    val numOfParts = args(1).toInt

    val conf = new SparkConf().setAppName("parsing_text_to_parquet_files")

    implicit val spark: SparkSession =
      SparkSession.builder().appName("ParquetFiles").config(conf).getOrCreate()

    val left: DataFrame = readMatrix(s"$basePath/raw/left")
    val right: DataFrame = readMatrix(s"$basePath/raw/right")

    left
      .repartition(numOfParts)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$basePath/common/left")
    right
      .repartition(numOfParts)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$basePath/common/right")
  }
}
