package edu.asu.tpch.queries

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

private[tpch] trait QueryOps {

  protected val log: Logger = Logger.getLogger("TPC_H_Queries")

  /**
   * Expression for the query
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame

  /**
   * Force the query computation by inducing an Action.
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  def computerQuery(tablesMap: Map[String, DataFrame]): Unit = {
    val currQuery = queryExpr(tablesMap)
    log.info(
      s"${this.getClass.getCanonicalName
        .split("\\.")
        .last
        .replace("$", "")} Spark SQL explain."
    )
    currQuery.explain(true)
    currQuery.count
  }

}
