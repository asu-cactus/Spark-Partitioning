package edu.asu.tpch.tables

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

private[tpch] case class Nation(
  N_NATIONKEY: Int,
  N_NAME: String,
  N_REGIONKEY: Int,
  N_COMMENT: String
)

private[tpch] object Nation extends TableOps {
  override protected def getSchema: StructType = Encoders.product[Nation].schema
  override protected def getRawDirName: String = "nation.tbl"
  override protected def getParquetDirName: String = "nation"
}
