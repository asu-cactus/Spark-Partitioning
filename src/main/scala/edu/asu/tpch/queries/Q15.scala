package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q15 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val lineitemDf = tablesMap("lineitem")
    val supplierDf = tablesMap("supplier")

    val decrease = udf((x: Double, y: Double) => x * (1 - y))

    val revenue = lineitemDf
      .filter(
        col(L_SHIPDATE) >= "1996-01-01" &&
          col(L_SHIPDATE) < "1996-04-01"
      )
      .select(
        col(L_SUPPKEY),
        decrease(col(L_EXTENDEDPRICE), col(L_DISCOUNT)).as("value")
      )
      .groupBy(L_SUPPKEY)
      .agg(sum("value").as("total"))

    revenue
      .agg(max("total").as("max_total"))
      .join(revenue, col("max_total") === revenue("total"))
      .join(supplierDf, col(L_SUPPKEY) === supplierDf(S_SUPPKEY))
      .select(S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE, "total")
      .sort(S_SUPPKEY)
  }

}
