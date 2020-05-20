package edu.asu.tpch.tables

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Encoders, Row}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.StructType

private[tpch] case class Lineitem(
  L_ORDERKEY: Int,
  L_PARTKEY: Int,
  L_SUPPKEY: Int,
  L_LINENUMBER: Int,
  L_QUANTITY: Double,
  L_EXTENDEDPRICE: Double,
  L_DISCOUNT: Double,
  L_TAX: Double,
  L_RETURNFLAG: String,
  L_LINESTATUS: String,
  L_SHIPDATE: String,
  L_COMMITDATE: String,
  L_RECEIPTDATE: String,
  L_SHIPINSTRUCT: String,
  L_SHIPMODE: String,
  L_COMMENT: String
)

object Lineitem extends TableOps {
  override protected def getSchema: StructType =
    Encoders.product[Lineitem].schema
  override protected def getRawDirName: String = "lineitem.tbl"
  override protected def getParquetDirName: String = "lineitem"
  override protected def transformRawDf(df: DataFrame): DataFrame =
    df.withColumn(
        L_SHIPDATE,
        to_date(col(L_SHIPDATE), dateFormat)
      )
      .withColumn(
        L_COMMITDATE,
        to_date(col(L_COMMITDATE), dateFormat)
      )
      .withColumn(
        L_RECEIPTDATE,
        to_date(col(L_RECEIPTDATE), dateFormat)
      )

  override protected def parquetParts(
    df: DataFrame,
    numOfParts: Int
  ): DataFrame =
    df.repartition(numOfParts, col(L_ORDERKEY))

  override protected def parquetBuckets(
    df: DataFrame,
    numOfParts: Int
  ): DataFrameWriter[Row] =
    df.repartition(numOfParts, col(L_ORDERKEY))
      .write
      .sortBy(L_ORDERKEY)
      .bucketBy(numOfParts, L_ORDERKEY)

}
