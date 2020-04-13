package edu.asu.tpch

import edu.asu.tpch.tables._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RawToParquet {

  /**
   * Job to read all the raw data for the TPC-H tables,
   * and covert the data to Parquet Format.
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      throw new IllegalArgumentException(
        "Base path for storing data and spark history log directory " +
          "is expected" +
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
      .setAppName(s"tpch_convert_RAW_PARQUET")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.history.fs.logDirectory", historyDir)
      .set("spark.eventLog.enabled", "true")
      .set("spark.default.parallelism", "80")
      .set("spark.eventLog.dir", historyDir)

    implicit val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // Parse and write Parquet Files
    Customer.rawToParquet(basePath)
    Lineitem.rawToParquet(basePath)
    Nation.rawToParquet(basePath)
    Orders.rawToParquet(basePath)
    Part.rawToParquet(basePath)
    Partsupp.rawToParquet(basePath)
    Region.rawToParquet(basePath)
    Supplier.rawToParquet(basePath)
  }

}
