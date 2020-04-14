package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q6 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val lineitemDf = tablesMap("lineitem")

    lineitemDf
      .filter(
        col(L_SHIPDATE) >= "1994-01-01" &&
          col(L_SHIPDATE) < "1995-01-01" && col(L_DISCOUNT) >= 0.05 &&
          col(L_DISCOUNT) <= 0.07 && col(L_QUANTITY) < 24
      )
      .agg(sum(col(L_EXTENDEDPRICE) * col(L_DISCOUNT)))
  }

}
