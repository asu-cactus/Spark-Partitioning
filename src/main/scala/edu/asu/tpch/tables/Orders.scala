package edu.asu.tpch.tables

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.StructType

private[tpch] case class Orders(
  O_ORDERKEY: Int,
  O_CUSTKEY: Int,
  O_ORDERSTATUS: String,
  O_TOTALPRICE: Double,
  O_ORDERDATE: String,
  O_ORDERPRIORITY: String,
  O_CLERK: String,
  O_SHIPPRIORITY: Int,
  O_COMMENT: String
)

private[tpch] object Orders extends TableOps {
  override protected def getSchema: StructType = Encoders.product[Orders].schema
  override protected def getRawDirName: String = "orders.tbl"
  override protected def getParquetDirName: String = "orders"
  override def rawToParquet(
    basePath: String
  )(implicit spark: SparkSession): Unit = {
    val rawDf = getRawTableDf(basePath, spark)
    rawDf
      .withColumn(
        "O_ORDERDATE",
        to_date(col("O_ORDERDATE"), dateFormat)
      )
      .repartition(numPartitions = 80)
      .write
      .parquet(s"$basePath/parquet/$getParquetDirName")
  }
}
