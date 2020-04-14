package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

private[tpch] object Q1 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val decrease = udf((x: Double, y: Double) => x * (1 - y))
    val increase = udf((x: Double, y: Double) => x * (1 + y))

    tablesMap("lineitem")
      .filter(col(L_SHIPDATE) <= "1998-09-02")
      .groupBy(L_RETURNFLAG, L_LINESTATUS)
      .agg(
        sum(col(L_QUANTITY)),
        sum(col(L_EXTENDEDPRICE)),
        sum(decrease(col(L_EXTENDEDPRICE), col(L_DISCOUNT))),
        sum(
          increase(
            decrease(col(L_EXTENDEDPRICE), col(L_DISCOUNT)),
            col(L_TAX)
          )
        ),
        avg(col(L_QUANTITY)),
        avg(col(L_EXTENDEDPRICE)),
        avg(col(L_DISCOUNT)),
        count(col(L_QUANTITY))
      )
      .sort(col(L_RETURNFLAG), col(L_LINESTATUS))
  }
}
