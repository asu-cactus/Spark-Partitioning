package edu.asu.tpch.queries

import java.sql.Date

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q8 extends QueryOps {

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
    val regionDf = tablesMap("region")
    val partDf = tablesMap("part")

    val getYear = udf((x: Date) => x.toString.substring(0, 4))
    val decrease = udf((x: Double, y: Double) => x * (1 - y))
    val isBrazil = udf((x: String, y: Double) => if (x == "BRAZIL") y else 0)

    val fregion = regionDf.filter(col(R_NAME) === "AMERICA")
    val forder = ordersDf
      .filter(
        col(O_ORDERDATE) <= "1996-12-31" && col(O_ORDERDATE) >= "1995-01-01"
      )
    val fpart = partDf.filter(col(P_TYPE) === "ECONOMY ANODIZED STEEL")

    val nat = nationDf
      .join(supplierDf, col(N_NATIONKEY) === supplierDf(S_NATIONKEY))

    val line = lineitemDf
      .select(
        col(L_PARTKEY),
        col(L_SUPPKEY),
        col(L_ORDERKEY),
        decrease(col(L_EXTENDEDPRICE), col(L_DISCOUNT)).as("volume")
      )
      .join(fpart, col(L_PARTKEY) === fpart(P_PARTKEY))
      .join(nat, col(L_SUPPKEY) === nat(S_SUPPKEY))

    nationDf
      .join(fregion, col(N_REGIONKEY) === fregion(R_REGIONKEY))
      .select(N_NATIONKEY)
      .join(customerDf, col(N_NATIONKEY) === customerDf(C_NATIONKEY))
      .select(C_CUSTKEY)
      .join(forder, col(C_CUSTKEY) === forder(O_CUSTKEY))
      .select(col(O_ORDERKEY), col(O_ORDERDATE))
      .join(line, col(O_ORDERKEY) === line(L_ORDERKEY))
      .select(
        getYear(col(O_ORDERDATE)).as("o_year"),
        col("volume"),
        isBrazil(col(N_NAME), col("volume")).as("case_volume")
      )
      .groupBy("o_year")
      .agg(sum(col("case_volume")) / sum("volume"))
      .sort(col("o_year"))
  }

}
