package edu.asu.tpch.tables

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

private[tpch] case class Customer(
  C_CUSTKEY: Int,
  C_NAME: String,
  C_ADDRESS: String,
  C_NATIONKEY: Int,
  C_PHONE: String,
  C_ACCTBAL: Double,
  C_MKTSEGMENT: String,
  C_COMMENT: String
)

private[tpch] object Customer extends TableOps {
  override protected def getSchema: StructType =
    Encoders.product[Customer].schema
  override protected def getRawDirName: String = "customer.tbl"
  override protected def getParquetDirName: String = "customer"
}
