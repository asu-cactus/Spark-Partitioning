package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q18 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val lineitemDf = tablesMap("lineitem")
    val ordersDf = tablesMap("orders")
    val customerDf = tablesMap("customer")

    lineitemDf
      .groupBy(L_ORDERKEY)
      .agg(sum(L_QUANTITY).as("sum_quantity"))
      .filter(col("sum_quantity") > 300)
      .select(col(L_ORDERKEY).as("key"), col("sum_quantity"))
      .join(ordersDf, ordersDf(O_ORDERKEY) === col("key"))
      .join(lineitemDf, col(O_ORDERKEY) === lineitemDf(L_ORDERKEY))
      .join(customerDf, customerDf(C_CUSTKEY) === col(O_CUSTKEY))
      .select(
        L_QUANTITY,
        C_NAME,
        C_CUSTKEY,
        O_ORDERKEY,
        O_ORDERDATE,
        O_TOTALPRICE
      )
      .groupBy(C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE)
      .agg(sum(L_QUANTITY))
      .sort(col(O_TOTALPRICE).desc, col(O_ORDERDATE))
      .limit(100)
  }

}
