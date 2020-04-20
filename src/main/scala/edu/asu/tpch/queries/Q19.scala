package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q19 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val lineitemDf = tablesMap("lineitem")
    val partDf = tablesMap("part")

    val sm = udf((x: String) => x.matches("SM CASE|SM BOX|SM PACK|SM PKG"))
    val md = udf { (x: String) =>
      x.matches("MED BAG|MED BOX|MED PKG|MED PACK")
    }
    val lg = udf((x: String) => x.matches("LG CASE|LG BOX|LG PACK|LG PKG"))

    val decrease = udf((x: Double, y: Double) => x * (1 - y))

    partDf
      .join(lineitemDf, col(L_PARTKEY) === col(P_PARTKEY))
      .filter(
        (col(L_SHIPMODE) === "AIR" || col(L_SHIPMODE) === "AIR REG") &&
          col(L_SHIPINSTRUCT) === "DELIVER IN PERSON"
      )
      .filter(
        ((col(P_BRAND) === "Brand#12") &&
          sm(col(P_CONTAINER)) &&
          col(L_QUANTITY) >= 1 && col(L_QUANTITY) <= 11 &&
          col(P_SIZE) >= 1 && col(P_SIZE) <= 5) ||
          ((col(P_BRAND) === "Brand#23") &&
            md(col(P_CONTAINER)) &&
            col(L_QUANTITY) >= 10 && col(L_QUANTITY) <= 20 &&
            col(P_SIZE) >= 1 && col(P_SIZE) <= 10) ||
          ((col(P_BRAND) === "Brand#34") &&
            lg(col(P_CONTAINER)) &&
            col(L_QUANTITY) >= 20 && col(L_QUANTITY) <= 30 &&
            col(P_SIZE) >= 1 && col(P_SIZE) <= 15)
      )
      .select(
        decrease(col(L_EXTENDEDPRICE), col(L_DISCOUNT)).as("volume")
      )
      .agg(sum("volume"))
  }

}
