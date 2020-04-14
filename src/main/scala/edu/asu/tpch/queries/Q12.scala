package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q12 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val ordersDf = tablesMap("orders")
    val lineitemDf = tablesMap("lineitem")

    val highPriority = udf { x: String =>
      if (x == "1-URGENT" || x == "2-HIGH") 1 else 0
    }
    val lowPriority = udf { x: String =>
      if (x != "1-URGENT" && x != "2-HIGH") 1 else 0
    }

    lineitemDf
      .filter(
        (col(L_SHIPMODE) === "MAIL" || col(L_SHIPMODE) === "SHIP") &&
          col(L_COMMITDATE) < col(L_RECEIPTDATE) &&
          col(L_SHIPDATE) < col(L_COMMITDATE) &&
          col(L_RECEIPTDATE) >= "1994-01-01" &&
          col(L_RECEIPTDATE) < "1995-01-01"
      )
      .join(ordersDf, col(L_ORDERKEY) === ordersDf(O_ORDERKEY))
      .select(L_SHIPMODE, O_ORDERPRIORITY)
      .groupBy(L_SHIPMODE)
      .agg(
        sum(highPriority(col(O_ORDERPRIORITY))).as("sum_highorderpriority"),
        sum(lowPriority(col(O_ORDERPRIORITY))).as("sum_loworderpriority")
      )
      .sort(L_SHIPMODE)
  }

}
