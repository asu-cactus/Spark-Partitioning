package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q4 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val ordersDf = tablesMap("orders")
    val lineitemDf = tablesMap("lineitem")

    val forders = ordersDf.filter(
      col(O_ORDERDATE) >= "1993-07-01" && col(O_ORDERDATE) < "1993-10-01"
    )
    val flineitems = lineitemDf
      .filter(col(L_COMMITDATE) < col(L_RECEIPTDATE))
      .select(L_ORDERKEY)
      .distinct

    flineitems
      .join(forders, col(L_ORDERKEY) === forders(O_ORDERKEY))
      .groupBy(col(O_ORDERPRIORITY))
      .agg(count(col(O_ORDERPRIORITY)))
      .sort(col(O_ORDERPRIORITY))
  }
}
