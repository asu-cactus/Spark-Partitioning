package edu.asu.tpch

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import edu.asu.tpch.tables._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RawToParquet {

  /**
   * Job to read all the raw data for the TPC-H tables,
   * and covert the data to Parquet Format.
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      throw new IllegalArgumentException(
        "Base path for storing data, spark history log directory, and " +
          "number of partitions is expected" +
          s"\nProvide: ${args.toList}"
      )
    }
    val basePath = args(0)
    val historyDir = args(1)
    val configPath = args(2)
    val configs = getConfigs(configPath)

    val conf = new SparkConf()
      .setAppName(s"tpch_convert_raw_parquet")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.history.fs.logDirectory", historyDir)
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", historyDir)

    implicit val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
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

    Customer.rawToParquetWithParts(basePath, configs)
    Lineitem.rawToParquetWithParts(basePath, configs)
    Nation.rawToParquetWithParts(basePath, configs)
    Orders.rawToParquetWithParts(basePath, configs)
    Part.rawToParquetWithParts(basePath, configs)
    Partsupp.rawToParquetWithParts(basePath, configs)
    Region.rawToParquetWithParts(basePath, configs)
    Supplier.rawToParquetWithParts(basePath, configs)

    // Parse and write to Hive tables
    Customer.rawToParquetWithBuckets(basePath, configs)
    Lineitem.rawToParquetWithBuckets(basePath, configs)
    Nation.rawToParquetWithBuckets(basePath, configs)
    Orders.rawToParquetWithBuckets(basePath, configs)
    Part.rawToParquetWithBuckets(basePath, configs)
    Partsupp.rawToParquetWithBuckets(basePath, configs)
    Region.rawToParquetWithBuckets(basePath, configs)
    Supplier.rawToParquetWithBuckets(basePath, configs)
  }

  /**
   * Method to read configurations for partitioning the
   * TPC-H tables
   *
   * @param configPath File path for the configurations.
   * @return [[Config]] object
   */
  def getConfigs(configPath: String): Config =
    ConfigFactory
      .parseFile(new File(configPath))
      .getConfig("tpch")

}
