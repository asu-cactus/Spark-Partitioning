package edu.asu.sqlpartitioning

import edu.asu.sqlpartitioning.utils.Parser.readMatrix
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object TextToParquetFiles {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      throw new IllegalArgumentException(
        "Base path for storing data is expected." +
          s"\nProvide: ${args.toList}"
      )
    }

    val basePath = args(0)

    val conf = new SparkConf().setAppName("parsing_text_to_parquet_files")

    implicit val spark: SparkSession =
      SparkSession.builder().appName("ParquetFiles").config(conf).getOrCreate()

    val left: DataFrame = readMatrix(s"$basePath/raw/left")
    val right: DataFrame = readMatrix(s"$basePath/raw/right")

    left
      .repartition(100)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$basePath/common/left")
    right
      .repartition(100)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$basePath/common/right")
  }
}
