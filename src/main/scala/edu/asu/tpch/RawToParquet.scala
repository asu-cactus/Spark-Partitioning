package edu.asu.tpch

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
    val numOfParts = args(2).toInt

    val conf = new SparkConf()
      .setAppName(s"tpch_convert_raw_parquet")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.history.fs.logDirectory", historyDir)
      .set("spark.eventLog.enabled", "true")
      .set("spark.sql.shuffle.partitions", numOfParts.toString)
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

    Customer.rawToParquetWithParts(basePath, numOfParts)
    Lineitem.rawToParquetWithParts(basePath, numOfParts)
    Nation.rawToParquetWithParts(basePath, numOfParts)
    Orders.rawToParquetWithParts(basePath, numOfParts)
    Part.rawToParquetWithParts(basePath, numOfParts)
    Partsupp.rawToParquetWithParts(basePath, numOfParts)
    Region.rawToParquetWithParts(basePath, numOfParts)
    Supplier.rawToParquetWithParts(basePath, numOfParts)

    // Parse and write to Hive tables
    Customer.rawToParquetWithBuckets(basePath, numOfParts)
    Lineitem.rawToParquetWithBuckets(basePath, numOfParts)
    Nation.rawToParquetWithBuckets(basePath, numOfParts)
    Orders.rawToParquetWithBuckets(basePath, numOfParts)
    Part.rawToParquetWithBuckets(basePath, numOfParts)
    Partsupp.rawToParquetWithBuckets(basePath, numOfParts)
    Region.rawToParquetWithBuckets(basePath, numOfParts)
    Supplier.rawToParquetWithBuckets(basePath, numOfParts)
  }

}
