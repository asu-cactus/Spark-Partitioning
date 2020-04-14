package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q14 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val lineitemDf = tablesMap("lineitem")
    val partDf = tablesMap("part")

    val reduce = udf((x: Double, y: Double) => x * (1 - y))
    val promo = udf { (x: String, y: Double) =>
      if (x.startsWith("PROMO")) y else 0
    }

    partDf
      .join(
        lineitemDf,
        col(L_PARTKEY) === col(P_PARTKEY) &&
          col(L_SHIPDATE) >= "1995-09-01" && col(L_SHIPDATE) < "1995-10-01"
      )
      .select(
        col(P_TYPE),
        reduce(col(L_EXTENDEDPRICE), col(L_DISCOUNT)).as("value")
      )
      .agg(
        sum(promo(col(P_TYPE), col("value"))) *
          100 / sum(col("value"))
      )
  }

}
