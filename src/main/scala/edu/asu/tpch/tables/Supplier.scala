package edu.asu.tpch.tables

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

private[tpch] case class Supplier(
  S_SUPPKEY: Int,
  S_NAME: String,
  S_ADDRESS: String,
  S_NATIONKEY: Int,
  S_PHONE: String,
  S_ACCTBAL: Double,
  S_COMMENT: String
)

private[tpch] object Supplier extends TableOps {
  override protected def getSchema: StructType =
    Encoders.product[Supplier].schema
  override protected def getRawDirName: String = "supplier.tbl"
  override protected def getParquetDirName: String = "supplier"
}
