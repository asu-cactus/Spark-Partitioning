package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q5 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val nationDf = tablesMap("nation")
    val supplierDf = tablesMap("supplier")
    val ordersDf = tablesMap("orders")
    val regionDf = tablesMap("region")
    val lineitemDf = tablesMap("lineitem")
    val customerDf = tablesMap("customer")

    val decrease = udf((x: Double, y: Double) => x * (1 - y))

    val forders = ordersDf
      .filter(
        col(O_ORDERDATE) < "1995-01-01" && col(O_ORDERDATE) >= "1994-01-01"
      )

    regionDf
      .filter(col(R_NAME) === "ASIA")
      .join(nationDf, col(R_REGIONKEY) === nationDf(N_REGIONKEY))
      .join(supplierDf, col(N_NATIONKEY) === supplierDf(S_NATIONKEY))
      .join(lineitemDf, col(S_SUPPKEY) === lineitemDf(L_SUPPKEY))
      .select(N_NAME, L_EXTENDEDPRICE, L_DISCOUNT, L_ORDERKEY, S_NATIONKEY)
      .join(forders, col(L_ORDERKEY) === forders(O_ORDERKEY))
      .join(
        customerDf,
        col(O_CUSTKEY) === customerDf(C_CUSTKEY) &&
          col(S_NATIONKEY) === customerDf(C_NATIONKEY)
      )
      .select(
        col(N_NAME),
        decrease(col(L_EXTENDEDPRICE), col(L_DISCOUNT)).as("value")
      )
      .groupBy(col(N_NAME))
      .agg(sum(col("value")).as("revenue"))
      .sort(col("revenue").desc)
  }

}
