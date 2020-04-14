package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q3 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val customerDf = tablesMap("customer")
    val ordersDf = tablesMap("orders")
    val lineitemDf = tablesMap("lineitem")

    val decrease = udf((x: Double, y: Double) => x * (1 - y))

    val fcust = customerDf.filter(col(C_MKTSEGMENT) === "BUILDING")
    val forders = ordersDf.filter(col(O_ORDERDATE) < "1995-03-15")
    val flineitems = lineitemDf.filter(col(L_SHIPDATE) > "1995-03-15")

    fcust
      .join(forders, col(C_CUSTKEY) === forders(O_CUSTKEY))
      .select(O_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY)
      .join(flineitems, col(O_ORDERKEY) === flineitems(L_ORDERKEY))
      .select(
        col(L_ORDERKEY),
        decrease(col(L_EXTENDEDPRICE), col(L_DISCOUNT)).as("volume"),
        col(O_ORDERDATE),
        col(O_SHIPPRIORITY)
      )
      .groupBy(L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY)
      .agg(sum(col("volume")).as("revenue"))
      .sort(col("revenue").desc, col(O_ORDERDATE))
      .limit(10)
  }
}
