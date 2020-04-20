package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q11 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val nationDf = tablesMap("nation")
    val supplierDf = tablesMap("supplier")
    val partsuppDf = tablesMap("partsupp")

    val mul = udf((x: Double, y: Int) => x * y)
    val mul01 = udf((x: Double) => x * 0.0001)

    val tmp = nationDf
      .filter(col(N_NAME) === "GERMANY")
      .join(supplierDf, col(N_NATIONKEY) === supplierDf(S_NATIONKEY))
      .select(S_SUPPKEY)
      .join(partsuppDf, col(S_SUPPKEY) === partsuppDf(PS_SUPPKEY))
      .select(
        col(PS_PARTKEY),
        mul(col(PS_SUPPLYCOST), col(PS_AVAILQTY)).as("value")
      )

    val sumRes = tmp.agg(sum("value").as("total_value"))

    tmp
      .groupBy(PS_PARTKEY)
      .agg(sum("value").as("part_value"))
      .join(
        sumRes,
        col("part_value") > mul01(col("total_value"))
      )
      .sort(col("part_value").desc)
  }

}
