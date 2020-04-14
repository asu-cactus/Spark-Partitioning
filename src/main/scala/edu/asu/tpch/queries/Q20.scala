package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q20 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val lineitemDf = tablesMap("lineitem")
    val nationDf = tablesMap("nation")
    val supplierDf = tablesMap("supplier")
    val partDf = tablesMap("part")
    val partsuppDf = tablesMap("partsupp")

    val forest = udf((x: String) => x.startsWith("forest"))

    val flineitem = lineitemDf
      .filter(col(L_SHIPDATE) >= "1994-01-01" && col(L_SHIPDATE) < "1995-01-01")
      .groupBy(col(L_PARTKEY), col(L_SUPPKEY))
      .agg((sum(col(L_QUANTITY)) * 0.5).as("sum_quantity"))

    val fnation = nationDf
      .filter(col(N_NAME) === "CANADA")
    val nat_supp = supplierDf
      .select(S_SUPPKEY, S_NAME, S_NATIONKEY, S_ADDRESS)
      .join(fnation, col(S_NATIONKEY) === fnation(N_NATIONKEY))

    partDf
      .filter(forest(col(P_NAME)))
      .select(P_PARTKEY)
      .distinct
      .join(partsuppDf, col(P_PARTKEY) === partsuppDf(PS_PARTKEY))
      .join(
        flineitem,
        col(PS_SUPPKEY) === flineitem(L_SUPPKEY) &&
          col(PS_PARTKEY) === flineitem(L_PARTKEY)
      )
      .filter(col(PS_AVAILQTY) > col("sum_quantity"))
      .select(PS_SUPPKEY)
      .distinct
      .join(nat_supp, col(PS_SUPPKEY) === nat_supp(S_SUPPKEY))
      .select(S_NAME, S_ADDRESS)
      .sort(S_NAME)
  }

}
