package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q2 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val nationDf = tablesMap("nation")
    val supplierDf = tablesMap("supplier")
    val partsuppDf = tablesMap("partsupp")
    val partDf = tablesMap("part")

    val europe = tablesMap("region")
      .filter(col(R_NAME) === "EUROPE")
      .join(nationDf, col(R_REGIONKEY) === nationDf(N_REGIONKEY))
      .join(supplierDf, col(N_NATIONKEY) === supplierDf(S_NATIONKEY))
      .join(partsuppDf, supplierDf(S_SUPPKEY) === partsuppDf(PS_SUPPKEY))

    val brass = partDf
      .filter(partDf(P_SIZE) === 15 && partDf(P_TYPE).endsWith("BRASS"))
      .join(europe, europe(PS_PARTKEY) === col(P_PARTKEY))

    val minCost = brass
      .groupBy(brass(PS_PARTKEY))
      .agg(min(PS_SUPPLYCOST).as("min"))

    brass
      .join(minCost, brass(PS_PARTKEY) === minCost(PS_PARTKEY))
      .filter(brass(PS_SUPPLYCOST) === minCost("min"))
      .select(
        S_ACCTBAL,
        S_NAME,
        N_NAME,
        P_PARTKEY,
        P_MFGR,
        S_ADDRESS,
        S_PHONE,
        S_COMMENT
      )
      .sort(col(S_ACCTBAL).desc, col(N_NAME), col(S_NAME), col(P_PARTKEY))
      .limit(100)
  }

}
