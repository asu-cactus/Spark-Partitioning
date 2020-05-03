package edu.asu.sqlbucketing

import edu.asu.sqlbucketing.experiments._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object Main {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      throw new IllegalArgumentException(
        "Base path for storing data, spark history log directory, experiment " +
          "to execute and number of partitions is expected." +
          s"\nProvide: ${args.toList}"
      )
    }
    val basePath = args(0)
    val historyDir = args(1)
    val numOfParts = args(2).toInt
    val experiment = args(3)

    Logger.getLogger("org.spark_project").setLevel(Level.WARN)
    Logger.getLogger("org.apache").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("com").setLevel(Level.WARN)

    implicit val log: Logger = Logger.getLogger("MatrixMultiplication")
    System.setProperty("spark.hadoop.dfs.replication", "1")

    val conf = new SparkConf()
      .setAppName(s"bucket_multiplication_$experiment")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.history.fs.logDirectory", historyDir)
      .set("spark.eventLog.enabled", "true")
      .set("spark.default.parallelism", "80")
      .set("spark.eventLog.dir", historyDir)

    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .appName("Table_Buckets")
        .config(conf)
        .getOrCreate()

    experiment match {
      case "e1" => new E1(numOfParts).execute(basePath)
      case "e2" => new E2(numOfParts).execute(basePath)
    }

    spark.sql("DROP TABLE left")
    spark.sql("DROP TABLE right")
    spark.sql("DROP TABLE matrix_op")

  }

}
