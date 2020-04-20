package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q16 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val supplierDf = tablesMap("supplier")
    val partsuppDf = tablesMap("partsupp")
    val partDf = tablesMap("part")

    val complains = udf((x: String) => x.matches(".*Customer.*Complaints.*"))
    val polished = udf((x: String) => x.startsWith("MEDIUM POLISHED"))
    val numbers =
      udf((x: Int) => x.toString.matches("49|14|23|45|19|3|36|9"))

    val fparts = partDf
      .filter(
        (col(P_BRAND) =!= "Brand#45") && !polished(col(P_TYPE)) &&
          numbers(col(P_SIZE))
      )
      .select(P_PARTKEY, P_BRAND, P_TYPE, P_SIZE)

    supplierDf
      .filter(!complains(col(S_COMMENT)))
      .join(partsuppDf, col(S_SUPPKEY) === partsuppDf(PS_SUPPKEY))
      .select(PS_PARTKEY, PS_SUPPKEY)
      .join(fparts, col(PS_PARTKEY) === fparts(P_PARTKEY))
      .groupBy(P_BRAND, P_TYPE, P_SIZE)
      .agg(countDistinct(col(PS_SUPPKEY)).as("supplier_count"))
      .sort(
        col("supplier_count").desc,
        col(P_BRAND),
        col(P_TYPE),
        col(P_SIZE)
      )
  }

}
