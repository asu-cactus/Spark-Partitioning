package edu.asu.tpch.queries

import org.apache.spark.sql.DataFrame

private[tpch] trait QueryOps {

  /**
   * Expression for the query
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame

  /**
   * Force the query computation by inducing an Action.
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  def computerQuery(tablesMap: Map[String, DataFrame]): Unit =
    queryExpr(tablesMap).count

}
