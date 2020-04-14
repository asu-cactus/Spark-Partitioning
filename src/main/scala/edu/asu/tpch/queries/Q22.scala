package edu.asu.tpch.queries

import edu.asu.tpch.tables.AllColNames._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

private[tpch] object Q22 extends QueryOps {

  /**
   * Expression for the query
   *
   * @param tablesMap Map of TPC-H tables in DataFrame format
   */
  override def queryExpr(tablesMap: Map[String, DataFrame]): DataFrame = {
    val customerDf = tablesMap("customer")
    val ordersDf = tablesMap("orders")

    val sub2 = udf((x: String) => x.substring(0, 2))
    val phone = udf((x: String) => x.matches("13|31|23|29|30|18|17"))

    val fcustomer = customerDf
      .select(
        col(C_ACCTBAL),
        col(C_CUSTKEY),
        sub2(col(C_PHONE)).as("cntrycode")
      )
      .filter(phone(col("cntrycode")))

    val avg_customer = fcustomer
      .filter(col(C_ACCTBAL) > 0.0)
      .agg(avg(C_ACCTBAL).as("avg_acctbal"))

    ordersDf
      .groupBy(O_CUSTKEY)
      .agg(col(O_CUSTKEY))
      .select(O_CUSTKEY)
      .join(fcustomer, col(O_CUSTKEY) === fcustomer(C_CUSTKEY), "right_outer")
      .filter(col(O_CUSTKEY).isNull)
      .join(avg_customer)
      .filter(col(C_ACCTBAL) > col("avg_acctbal"))
      .groupBy(col("cntrycode"))
      .agg(count(col(C_ACCTBAL)), sum(col(C_ACCTBAL)))
      .sort("cntrycode")
  }

}
