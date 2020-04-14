package edu.asu.tpch.queries

import java.sql.Date

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q9 extends QueryOps {

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
    val partsuppDf = tablesMap("partsupp")
    val partDf = tablesMap("part")

    val getYear = udf((x: Date) => x.toString.substring(0, 4))

    val linePart = partDf
      .filter(col(P_NAME).contains("green"))
      .join(lineitemDf, col(P_PARTKEY) === lineitemDf(L_PARTKEY))

    val natSup =
      nationDf.join(supplierDf, col(N_NATIONKEY) === supplierDf(S_NATIONKEY))

    linePart
      .join(natSup, col(L_SUPPKEY) === natSup(S_SUPPKEY))
      .join(
        partsuppDf,
        col(L_SUPPKEY) === partsuppDf(PS_SUPPKEY)
          && col(L_PARTKEY) === partsuppDf(PS_PARTKEY)
      )
      .join(ordersDf, col(L_ORDERKEY) === ordersDf(O_ORDERKEY))
      .select(
        col(N_NAME),
        getYear(col(O_ORDERDATE)).as("o_year"),
        (
          col(L_EXTENDEDPRICE) * (lit(1) - col(L_DISCOUNT)) -
            col(PS_SUPPLYCOST) * col(L_QUANTITY)
        ).as("amount")
      )
      .groupBy(N_NAME, "o_year")
      .agg(sum(col("amount")))
      .sort(col(N_NAME), col("o_year").desc)
  }

}
