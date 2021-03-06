package edu.asu.tpch.tables

import edu.asu.tpch.tables.AllColNames.O_ORDERDATE
import org.apache.spark.sql.{DataFrame, Encoders}
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
  override protected def getTableName: String = "orders"
  override protected def transformRawDf(df: DataFrame): DataFrame =
    df.withColumn(
      O_ORDERDATE,
      to_date(col(O_ORDERDATE), dateFormat)
    )
}
