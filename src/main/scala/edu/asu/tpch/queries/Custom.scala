package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object Custom extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val ordersDf = tablesMap("orders")
    val lineitemDf = tablesMap("lineitem")

    lineitemDf
      .join(ordersDf, col(L_ORDERKEY) === ordersDf(O_ORDERKEY))
      .groupBy(O_CUSTKEY)
      .agg(sum(L_DISCOUNT))
  }

}
