package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

private[tpch] object Q17 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val partDf = tablesMap("part")
    val lineitemDf = tablesMap("lineitem")

    val mul02 = udf((x: Double) => x * 0.2)

    val flineitem = lineitemDf
      .select(L_PARTKEY, L_QUANTITY, L_EXTENDEDPRICE)

    val fpart = partDf
      .filter(col(P_BRAND) === "Brand#23" && col(P_CONTAINER) === "MED BOX")
      .select(P_PARTKEY)
      .join(
        flineitem,
        col(P_PARTKEY) === flineitem(L_PARTKEY),
        joinType = "left_outer"
      )

    fpart
      .groupBy(P_PARTKEY)
      .agg(mul02(avg(col(L_QUANTITY))).as("avg_quantity"))
      .select(col(P_PARTKEY).as("key"), col("avg_quantity"))
      .join(fpart, col("key") === fpart(P_PARTKEY))
      .filter(col(L_QUANTITY) < col("avg_quantity"))
      .agg(sum(col(L_EXTENDEDPRICE)) / 7.0)
  }

}
