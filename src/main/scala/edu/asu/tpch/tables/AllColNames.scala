package edu.asu.tpch.tables

private[tpch] object AllColNames {
  val C_CUSTKEY = "C_CUSTKEY"
  val C_NAME = "C_NAME"
  val C_ADDRESS = "C_ADDRESS"
  val C_NATIONKEY = "C_NATIONKEY"
  val C_PHONE = "C_PHONE"
  val C_ACCTBAL = "C_ACCTBAL"
  val C_MKTSEGMENT = "C_MKTSEGMENT"
  val C_COMMENT = "C_COMMENT"

  val L_ORDERKEY = "L_ORDERKEY"
  val L_PARTKEY = "L_PARTKEY"
  val L_SUPPKEY = "L_SUPPKEY"
  val L_LINENUMBER = "L_LINENUMBER"
  val L_QUANTITY = "L_QUANTITY"
  val L_EXTENDEDPRICE = "L_EXTENDEDPRICE"
  val L_DISCOUNT = "L_DISCOUNT"
  val L_TAX = "L_TAX"
  val L_RETURNFLAG = "L_RETURNFLAG"
  val L_LINESTATUS = "L_LINESTATUS"
  val L_SHIPDATE = "L_SHIPDATE"
  val L_COMMITDATE = "L_COMMITDATE"
  val L_RECEIPTDATE = "L_RECEIPTDATE"
  val L_SHIPINSTRUCT = "L_SHIPINSTRUCT"
  val L_SHIPMODE = "L_SHIPMODE"
  val L_COMMENT = "L_COMMENT"

  val N_NATIONKEY = "N_NATIONKEY"
  val N_NAME = "N_NAME"
  val N_REGIONKEY = "N_REGIONKEY"
  val N_COMMENT = "N_COMMENT"

  val O_ORDERKEY = "O_ORDERKEY"
  val O_CUSTKEY = "O_CUSTKEY"
  val O_ORDERSTATUS = "O_ORDERSTATUS"
  val O_TOTALPRICE = "O_TOTALPRICE"
  val O_ORDERDATE = "O_ORDERDATE"
  val O_ORDERPRIORITY = "O_ORDERPRIORITY"
  val O_CLERK = "O_CLERK"
  val O_SHIPPRIORITY = "O_SHIPPRIORITY"
  val O_COMMENT = "O_COMMENT"

  val P_PARTKEY = "P_PARTKEY"
  val P_NAME = "P_NAME"
  val P_MFGR = "P_MFGR"
  val P_BRAND = "P_BRAND"
  val P_TYPE = "P_TYPE"
  val P_SIZE = "P_SIZE"
  val P_CONTAINER = "P_CONTAINER"
  val P_RETAILPRICE = "P_RETAILPRICE"
  val P_COMMENT = "P_COMMENT"

  val PS_PARTKEY = "PS_PARTKEY"
  val PS_SUPPKEY = "PS_SUPPKEY"
  val PS_AVAILQTY = "PS_AVAILQTY"
  val PS_SUPPLYCOST = "PS_SUPPLYCOST"
  val PS_COMMENT = "PS_COMMENT"

  val R_REGIONKEY = "R_REGIONKEY"
  val R_NAME = "R_NAME"
  val R_COMMENT = "R_COMMENT"

  val S_SUPPKEY = "S_SUPPKEY"
  val S_NAME = "S_NAME"
  val S_ADDRESS = "S_ADDRESS"
  val S_NATIONKEY = "S_NATIONKEY"
  val S_PHONE = "S_PHONE"
  val S_ACCTBAL = "S_ACCTBAL"
  val S_COMMENT = "S_COMMENT"
}
