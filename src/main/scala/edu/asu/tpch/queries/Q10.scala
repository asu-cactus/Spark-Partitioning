package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q10 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val nationDf = tablesMap("nation")
    val ordersDf = tablesMap("orders")
    val lineitemDf = tablesMap("lineitem")
    val customerDf = tablesMap("customer")

    val decrease = udf((x: Double, y: Double) => x * (1 - y))

    val flineitem = lineitemDf.filter(col(L_RETURNFLAG) === "R")

    ordersDf
      .filter(
        col(O_ORDERDATE) < "1994-01-01" && col(O_ORDERDATE) >= "1993-10-01"
      )
      .join(customerDf, col(O_CUSTKEY) === customerDf(C_CUSTKEY))
      .join(nationDf, col(C_NATIONKEY) === nationDf(N_NATIONKEY))
      .join(flineitem, col(O_ORDERKEY) === flineitem(L_ORDERKEY))
      .select(
        col(C_CUSTKEY),
        col(C_NAME),
        decrease(col(L_EXTENDEDPRICE), col(L_DISCOUNT)).as("volume"),
        col(C_ACCTBAL),
        col(N_NAME),
        col(C_ADDRESS),
        col(C_PHONE),
        col(C_COMMENT)
      )
      .groupBy(
        col(C_CUSTKEY),
        col(C_NAME),
        col(C_ACCTBAL),
        col(N_NAME),
        col(C_ADDRESS),
        col(C_PHONE),
        col(C_COMMENT)
      )
      .agg(sum("volume").as("revenue"))
      .sort(col("revenue").desc)
      .limit(20)
  }

}
