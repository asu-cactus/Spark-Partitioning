package edu.asu.tpch

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import edu.asu.tpch.tables._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.microsoft.hyperspace._

object RawToParquet {

  /**
   * Job to read all the raw data for the TPC-H tables,
   * and covert the data to Parquet Format.
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      throw new IllegalArgumentException(
        "Base path for storing data, and configuration path is expected" +
          s"\nProvide: ${args.toList}"
      )
    }
    val basePath = args(0)
    val configPath = args(1)
    val configs = getConfigs(configPath)

    val conf = new SparkConf().setAppName(s"tpch_convert_csv_to_structured")

    implicit val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()
      .enableHyperspace()

    implicit val hyperspace: Hyperspace = Hyperspace()

    // Parse and write Parquet Files with Hyperspace indices
    Customer.rawToParquetHyperspace(basePath, configs, hyperspace)
    Lineitem.rawToParquetHyperspace(basePath, configs, hyperspace)
    Nation.rawToParquetHyperspace(basePath, configs, hyperspace)
    Orders.rawToParquetHyperspace(basePath, configs, hyperspace)
    Part.rawToParquetHyperspace(basePath, configs, hyperspace)
    Partsupp.rawToParquetHyperspace(basePath, configs, hyperspace)
    Region.rawToParquetHyperspace(basePath, configs, hyperspace)
    Supplier.rawToParquetHyperspace(basePath, configs, hyperspace)

    // Parse and write Parquet Files with given key
    Customer.rawToParquetWithParts(basePath, configs)
    Lineitem.rawToParquetWithParts(basePath, configs)
    Nation.rawToParquetWithParts(basePath, configs)
    Orders.rawToParquetWithParts(basePath, configs)
    Part.rawToParquetWithParts(basePath, configs)
    Partsupp.rawToParquetWithParts(basePath, configs)
    Region.rawToParquetWithParts(basePath, configs)
    Supplier.rawToParquetWithParts(basePath, configs)

    // Parse and write to Hive tables with buckets on given key
//    Customer.rawToTableWithBuckets(basePath, configs)
//    Lineitem.rawToTableWithBuckets(basePath, configs)
//    Nation.rawToTableWithBuckets(basePath, configs)
//    Orders.rawToTableWithBuckets(basePath, configs)
//    Part.rawToTableWithBuckets(basePath, configs)
//    Partsupp.rawToTableWithBuckets(basePath, configs)
//    Region.rawToTableWithBuckets(basePath, configs)
//    Supplier.rawToTableWithBuckets(basePath, configs)
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
