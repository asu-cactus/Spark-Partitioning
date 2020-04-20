package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q21 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val nationDf = tablesMap("nation")
    val supplierDf = tablesMap("supplier")
    val ordersDf = tablesMap("orders")
    val lineitemDf = tablesMap("lineitem")

    val fsupplier = supplierDf.select(S_SUPPKEY, S_NATIONKEY, S_NAME)

    val plineitem =
      lineitemDf.select(L_SUPPKEY, L_ORDERKEY, L_RECEIPTDATE, L_COMMITDATE)

    val flineitem = plineitem.filter(col(L_RECEIPTDATE) > col(L_COMMITDATE))

    val line1 = plineitem
      .groupBy(L_ORDERKEY)
      .agg(
        countDistinct(L_SUPPKEY).as("suppkey_count"),
        max(L_SUPPKEY).as("suppkey_max")
      )
      .select(
        col(L_ORDERKEY).as("key"),
        col("suppkey_count"),
        col("suppkey_max")
      )

    val line2 = flineitem
      .groupBy(L_ORDERKEY)
      .agg(
        countDistinct(L_SUPPKEY).as("suppkey_count"),
        max(L_SUPPKEY).as("suppkey_max")
      )
      .select(
        col(L_ORDERKEY).as("key"),
        col("suppkey_count"),
        col("suppkey_max")
      )

    val forder = ordersDf
      .select(O_ORDERKEY, O_ORDERSTATUS)
      .filter(col(O_ORDERSTATUS) === "F")

    nationDf
      .filter(col(N_NAME) === "SAUDI ARABIA")
      .join(fsupplier, col(N_NATIONKEY) === fsupplier(S_NATIONKEY))
      .join(flineitem, col(S_SUPPKEY) === flineitem(L_SUPPKEY))
      .join(forder, col(L_ORDERKEY) === forder(O_ORDERKEY))
      .join(line1, col(L_ORDERKEY) === line1("key"))
      .filter(
        col("suppkey_count") > 1 || (col("suppkey_count") === 1 &&
          col(L_SUPPKEY) === col("suppkey_max"))
      )
      .select(S_NAME, L_ORDERKEY, L_SUPPKEY)
      .join(line2, col(L_ORDERKEY) === line2("key"), "left_outer")
      .select(
        S_NAME,
        L_ORDERKEY,
        L_SUPPKEY,
        "suppkey_count",
        "suppkey_max"
      )
      .filter(
        col("suppkey_count") === 1 && col(L_SUPPKEY) === col("suppkey_max")
      )
      .groupBy(S_NAME)
      .agg(count(L_SUPPKEY).as("numwait"))
      .sort(col("numwait").desc, col(S_NAME))
      .limit(100)
  }

}
