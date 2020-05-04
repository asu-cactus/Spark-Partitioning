package edu.asu.sqlpartitioning

import edu.asu.sqlpartitioning.utils.Parser.readMatrix
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object TextToParquetFiles {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      throw new IllegalArgumentException(
        "Base path for storing data and spark history log directory are expected." +
          s"\nProvide: ${args.toList}"
      )
    }

    val basePath = args(0)
    val historyDir = args(1)

    Logger.getLogger("org.spark_project").setLevel(Level.WARN)
    Logger.getLogger("org.apache").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("com").setLevel(Level.WARN)
    System.setProperty("spark.hadoop.dfs.replication", "1")

    val conf = new SparkConf()
      .setAppName("parsing_text_to_parquet_files")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.history.fs.logDirectory", historyDir)
      .set("spark.eventLog.enabled", "true")
      .set("spark.default.parallelism", "80")
      .set("spark.eventLog.dir", historyDir)

    implicit val spark: SparkSession =
      SparkSession.builder().appName("ParquetFiles").config(conf).getOrCreate()

    val left: DataFrame = readMatrix(s"$basePath/raw/left")
    val right: DataFrame = readMatrix(s"$basePath/raw/right")

    left.write.parquet(s"$basePath/common/left")
    right.write.parquet(s"$basePath/common/right")
  }
}
