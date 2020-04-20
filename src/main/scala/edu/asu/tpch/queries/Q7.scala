package edu.asu.tpch.queries

import java.sql.Date

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q7 extends QueryOps {

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
    val customerDf = tablesMap("customer")

    val getYear = udf((x: Date) => x.toString.substring(0, 4))
    val decrease = udf((x: Double, y: Double) => x * (1 - y))

    val fnation =
      nationDf.filter(col(N_NAME) === "FRANCE" || col(N_NAME) === "GERMANY")
    val fline = lineitemDf.filter(
      col(L_SHIPDATE) >= "1995-01-01" && col(L_SHIPDATE) <= "1996-12-31"
    )

    val supNation = fnation
      .join(supplierDf, col(N_NATIONKEY) === supplierDf(S_NATIONKEY))
      .join(fline, col(S_SUPPKEY) === fline(L_SUPPKEY))
      .select(
        col(N_NAME).as("supp_nation"),
        col(L_ORDERKEY),
        col(L_EXTENDEDPRICE),
        col(L_DISCOUNT),
        col(L_SHIPDATE)
      )

    fnation
      .join(customerDf, col(N_NATIONKEY) === customerDf(C_NATIONKEY))
      .join(ordersDf, col(C_CUSTKEY) === ordersDf(O_CUSTKEY))
      .select(col(N_NAME).as("cust_nation"), col(O_ORDERKEY))
      .join(supNation, col(O_ORDERKEY) === supNation(L_ORDERKEY))
      .filter(
        col("supp_nation") === "FRANCE" &&
          col("cust_nation") === "GERMANY" ||
          col("supp_nation") === "GERMANY" &&
            col("cust_nation") === "FRANCE"
      )
      .select(
        col("supp_nation"),
        col("cust_nation"),
        getYear(col(L_SHIPDATE)).as("l_year"),
        decrease(col(L_EXTENDEDPRICE), col(L_DISCOUNT)).as("volume")
      )
      .groupBy(
        col("supp_nation"),
        col("cust_nation"),
        col("l_year")
      )
      .agg(sum(col("volume")).as("revenue"))
      .sort(
        col("supp_nation"),
        col("cust_nation"),
        col("l_year")
      )
  }

}
