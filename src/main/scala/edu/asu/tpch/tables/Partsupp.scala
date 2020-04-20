package edu.asu.tpch.tables

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

private[tpch] case class Partsupp(
  PS_PARTKEY: Int,
  PS_SUPPKEY: Int,
  PS_AVAILQTY: Int,
  PS_SUPPLYCOST: Double,
  PS_COMMENT: String
)

private[tpch] object Partsupp extends TableOps {
  override protected def getSchema: StructType =
    Encoders.product[Partsupp].schema
  override protected def getRawDirName: String = "partsupp.tbl"
  override protected def getParquetDirName: String = "partsupp"
}
