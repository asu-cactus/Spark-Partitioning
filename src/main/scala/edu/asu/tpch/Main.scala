package edu.asu.tpch

import edu.asu.tpch.queries._
import edu.asu.tpch.tables._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      throw new IllegalArgumentException(
        "Base path for storing data, spark history log directory, query " +
          "to execute are expected" +
          s"\nProvide: ${args.toList}"
      )
    }
    val basePath = args(0)
    val historyDir = args(1)
    val queryToRun = args(2)

    val queryNum =
      if (queryToRun == "all") {
        0
      } else {
        queryToRun.toInt
      }

    Logger.getLogger("org.spark_project").setLevel(Level.WARN)
    Logger.getLogger("org.apache").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("com").setLevel(Level.WARN)

    implicit val log: Logger = Logger.getLogger("TPC_H_Spark")
    System.setProperty("spark.hadoop.dfs.replication", "1")

    val conf = new SparkConf()
      .setAppName(s"tpch_performance_$queryToRun")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.history.fs.logDirectory", historyDir)
      .set("spark.eventLog.enabled", "true")
      .set("spark.default.parallelism", "80")
      .set("spark.eventLog.dir", historyDir)

    implicit val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val tableDfMap = Map(
      "customer" -> Customer.readTable(basePath),
      "lineitem" -> Lineitem.readTable(basePath),
      "nation" -> Nation.readTable(basePath),
      "orders" -> Orders.readTable(basePath),
      "part" -> Part.readTable(basePath),
      "partsupp" -> Partsupp.readTable(basePath),
      "region" -> Region.readTable(basePath),
      "supplier" -> Supplier.readTable(basePath)
    )

    val queryMap = Map(
      1 -> Q1,
      2 -> Q2,
      3 -> Q3,
      4 -> Q4,
      5 -> Q5,
      6 -> Q6,
      7 -> Q7,
      8 -> Q8,
      9 -> Q9,
      10 -> Q10,
      11 -> Q11,
      12 -> Q12,
      13 -> Q13,
      14 -> Q14,
      15 -> Q15,
      16 -> Q16,
      17 -> Q17,
      18 -> Q18,
      19 -> Q19,
      20 -> Q20,
      21 -> Q21,
      22 -> Q22
    )

    val queriesToRun = if (queryNum == 0) {
      queryMap.values.toSeq
    } else {
      Seq(queryMap(queryNum))
    }

    queriesToRun.foreach { q =>
      val (_, time) = timedBlock(q.computerQuery(tableDfMap))
      val queryTotalSeconds = time / math.pow(10, 3)
      val queryMinutes = (queryTotalSeconds / 60).toLong
      val querySeconds = (queryTotalSeconds % 60).toInt
      log.info(
        s"Time take by query ${q.getClass.getCanonicalName.split("\\.").last} " +
          s"is $queryMinutes minutes $querySeconds seconds"
      )
    }

  }

  /**
   * Method take an execution block and returns the time
   * required to execute the block of code in milliseconds,
   * along the return statement of the executed block.
   *
   * WARNING: Make sure you include the action on the
   * execution block, without an action Apache Spark
   * will continue building an execution graph and
   * actual execution time won't be clocked.
   *
   * @param block The block of code to be timed
   * @tparam R Return type of the given execution block
   * @return [[Tuple2]] of ([[R]], [[Int]])
   */
  def timedBlock[R](block: => R): (R, Long) = {
    val start = System.currentTimeMillis()
    val result = block
    val end = System.currentTimeMillis()
    val totalMilli = end - start
    val milliSeconds = totalMilli
    (result, milliSeconds)
  }

}
