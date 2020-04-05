package edu.asu.pagerank

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

object Main {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      throw new IllegalArgumentException(
        "Base path for storing data and spark history log directory are expected." +
          s"\nProvide: ${args.toList}"
      )
    }
    val basePath = args(0)
    val historyDir = args(1)
    val partStatus = args(2)

    if (!(partStatus == "CO_partitioned" || partStatus == "NO_partition")) {
      throw new IllegalArgumentException(
        "Allowed values for 3rd position arguments are - CO_partitioned" +
          s" or NO_partition. Provided: $partStatus"
      )
    }

    Logger.getLogger("org.spark_project").setLevel(Level.WARN)
    Logger.getLogger("org.apache").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("com").setLevel(Level.WARN)
    System.setProperty("spark.hadoop.dfs.replication", "1")

    val conf = new SparkConf()
      .setAppName(s"pagerank_$partStatus")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.history.fs.logDirectory", historyDir)
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", historyDir)

    implicit val sc: SparkContext = new SparkContext(conf)

    val linksRDD = parsePageRankData(s"$basePath/page_rank/raw")
    val ranksRDD = linksRDD.map(urlLinks => (urlLinks._1, 1.0))
    val numOfIters = 10

    val outputRanks = partStatus match {
      case "NO_partition" =>
        pageRankIteration(linksRDD, ranksRDD, numOfIters, 0)
      case "CO_partitioned" =>
        val hashParts = new HashPartitioner(partitions = 10)
        val partLinks = linksRDD.partitionBy(hashParts)
        val partRanks = ranksRDD.partitionBy(hashParts)
        pageRankIteration(partLinks, partRanks, numOfIters, 0)
    }

    outputRanks
      .map(ranks => s"${ranks._1}:${ranks.toString}")
      .saveAsTextFile(s"$basePath/page_rank/output/$partStatus")
  }

  @tailrec
  private def pageRankIteration(
    links: RDD[(String, Iterable[String])],
    ranks: RDD[(String, Double)],
    numOfIters: Int,
    acc: Int
  ): RDD[(String, Double)] =
    if (acc == numOfIters) {
      ranks
    } else {
      val contributions = links
        .join(ranks)
        .flatMap({
          case (_, (outLinks, rank)) =>
            val numOfOutLinks = outLinks.size
            outLinks.map(x => (x, rank / numOfOutLinks))
        })

      val updatedRanks = contributions
        .reduceByKey(_ + _)
        .mapValues(_ * 0.85 + 0.15)
      pageRankIteration(links, updatedRanks, numOfIters, acc + 1)
    }

  /**
   * Method to parse the raw csv/text file of the page rank data.
   *
   * @param rawDataPath Path to raw file
   * @param sc [[SparkContext]] Spark's application entry point.
   * @return [[RDD]] of [[(String, Iterable(String))]]
   */
  private def parsePageRankData(
    rawDataPath: String
  )(implicit sc: SparkContext): RDD[(String, Iterable[String])] = {
    val textRDD = sc.textFile(rawDataPath)
    textRDD
      .map(_.trim)
      .map { edge =>
        val nodes = edge.split("\\s+")
        (nodes.head, nodes.tail.head)
      }
      .distinct
      .groupByKey()
  }

}
