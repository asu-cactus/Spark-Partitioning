package edu.asu.tpch

import edu.asu.tpch.queries._
import edu.asu.tpch.tables._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.microsoft.hyperspace._

object Main {

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      throw new IllegalArgumentException(
        "Base path for storing data, query, " +
          "data partition type to read and number of partitions " +
          "are the expected parameters" +
          s"\nProvide: ${args.toList}"
      )
    }
    val basePath = args(0)
    val queryToRun = args(1)
    val partType = args(2)
    val numOfParts = args(3).toInt

    val queryNum = queryToRun match {
      case "all"      => 0
      case "custom"   => 23
      case in: String => in.toInt
    }

    implicit val log: Logger = Logger.getLogger("TPC_H_Spark")

    val conf = new SparkConf()
      .setAppName(s"tpch_${partType}_$queryToRun")
      .set("spark.sql.shuffle.partitions", numOfParts.toString)

    implicit val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()
      .enableHyperspace()

    implicit val hyperspace: Hyperspace = Hyperspace()

    val tableDfMap = partType match {
      case "hyperspace" =>
        Map(
          "customer" -> Customer.readTableHyperspace(basePath),
          "lineitem" -> Lineitem.readTableHyperspace(basePath),
          "nation" -> Nation.readTableHyperspace(basePath),
          "orders" -> Orders.readTableHyperspace(basePath),
          "part" -> Part.readTableHyperspace(basePath),
          "partsupp" -> Partsupp.readTableHyperspace(basePath),
          "region" -> Region.readTableHyperspace(basePath),
          "supplier" -> Supplier.readTableHyperspace(basePath)
        )

      case "parts" =>
        Map(
          "customer" -> Customer.readTableFromParts(basePath),
          "lineitem" -> Lineitem.readTableFromParts(basePath),
          "nation" -> Nation.readTableFromParts(basePath),
          "orders" -> Orders.readTableFromParts(basePath),
          "part" -> Part.readTableFromParts(basePath),
          "partsupp" -> Partsupp.readTableFromParts(basePath),
          "region" -> Region.readTableFromParts(basePath),
          "supplier" -> Supplier.readTableFromParts(basePath)
        )

      case "buckets" =>
        Map(
          "customer" -> Customer.readTableFromBuckets(basePath),
          "lineitem" -> Lineitem.readTableFromBuckets(basePath),
          "nation" -> Nation.readTableFromBuckets(basePath),
          "orders" -> Orders.readTableFromBuckets(basePath),
          "part" -> Part.readTableFromBuckets(basePath),
          "partsupp" -> Partsupp.readTableFromBuckets(basePath),
          "region" -> Region.readTableFromBuckets(basePath),
          "supplier" -> Supplier.readTableFromBuckets(basePath)
        )
    }

    val queryList = Seq(
      Q1,
      Q2,
      Q3,
      Q4,
      Q5,
      Q6,
      Q7,
      Q8,
      Q9,
      Q10,
      Q11,
      Q12,
      Q13,
      Q14,
      Q15,
      Q16,
      Q17,
      Q18,
      Q19,
      Q20,
      Q21,
      Q22,
      Custom
    )

    val queriesToRun = if (queryNum == 0) {
      queryList
    } else {
      Seq(queryList(queryNum - 1))
    }

    queriesToRun.foreach { q =>
      val (_, time) = timedBlock(q.computerQuery(tableDfMap))
      val queryTotalSeconds = time / math.pow(10, 3)
      val queryMinutes = (queryTotalSeconds / 60).toLong
      val querySeconds = (queryTotalSeconds % 60).toInt

      partType match {
        case "hyperspace" =>
          log.info(
            s"Time take by query ${q.getClass.getCanonicalName.split("\\.").last.replace("$", "")} " +
              s"is $queryMinutes minutes $querySeconds seconds"
          )
        case "parts" =>
          log.info(
            s"Time take by query ${q.getClass.getCanonicalName.split("\\.").last.replace("$", "")} " +
              s"after partition is $queryMinutes minutes $querySeconds seconds"
          )
        case "buckets" =>
          log.info(
            s"Time take by query ${q.getClass.getCanonicalName.split("\\.").last.replace("$", "")} " +
              s"after bucketing is $queryMinutes minutes $querySeconds seconds"
          )
      }
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
