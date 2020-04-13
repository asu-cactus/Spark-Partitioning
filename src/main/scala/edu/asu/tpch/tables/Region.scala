package edu.asu.tpch.tables

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

private[tpch] case class Region(
  R_REGIONKEY: Int,
  R_NAME: String,
  R_COMMENT: String
)

private[tpch] object Region extends TableOps {
  override protected def getSchema: StructType = Encoders.product[Region].schema
  override protected def getRawDirName: String = "region.tbl"
  override protected def getParquetDirName: String = "region"
}
