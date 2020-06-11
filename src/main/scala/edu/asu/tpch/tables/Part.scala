package edu.asu.tpch.tables

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

private[tpch] case class Part(
  P_PARTKEY: Int,
  P_NAME: String,
  P_MFGR: String,
  P_BRAND: String,
  P_TYPE: String,
  P_SIZE: Int,
  P_CONTAINER: String,
  P_RETAILPRICE: Double,
  P_COMMENT: String
)

private[tpch] object Part extends TableOps {
  override protected def getSchema: StructType = Encoders.product[Part].schema
  override protected def getRawDirName: String = "part.tbl"
  override protected def getTableName: String = "part"
}
