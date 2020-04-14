package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q13 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val ordersDf = tablesMap("orders")
    val customerDf = tablesMap("customer")

    val special = udf { x: String => x.matches(".*special.*requests.*") }

    customerDf
      .join(
        ordersDf,
        col(C_CUSTKEY) === ordersDf(O_CUSTKEY)
          && !special(ordersDf(O_COMMENT)),
        "left_outer"
      )
      .groupBy(O_CUSTKEY)
      .agg(count(O_ORDERKEY).as("c_count"))
      .groupBy("c_count")
      .agg(count(O_CUSTKEY).as("custdist"))
      .sort(col("custdist").desc, col("c_count").desc)
  }

}
