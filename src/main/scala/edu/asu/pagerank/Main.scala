package edu.asu.pagerank

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Main {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      throw new IllegalArgumentException(
        "Base path for storing data and spark history log directory are expected." +
          s"\nProvide: ${args.toList}"
      )
    }
    val basePath = args(0)
    val historyDir = args(1)
    val partStatus = args(2)
    val numOfIters = args(3).toInt

    if (!(partStatus == "with_partition" || partStatus == "no_partition")) {
      throw new IllegalArgumentException(
        "Allowed values for 3rd position arguments are - with_partition" +
          s" or no_partition. Provided: $partStatus"
      )
    }

    val conf = new SparkConf()
      .setAppName(s"pagerank_$partStatus")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.history.fs.logDirectory", historyDir)
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", historyDir)

    implicit val sc: SparkContext = new SparkContext(conf)

    val linksRDD = parsePageRankData(s"$basePath/page_rank/raw")
    val ranksRDD = linksRDD.map(urlLinks => (urlLinks._1, 1.0))

    val outputRanks = partStatus match {
      case "no_partition" =>
        pageRankIterations(linksRDD, ranksRDD, numOfIters)
      case "with_partition" =>
        val hashParts = new HashPartitioner(partitions = 16)
        val partLinks = linksRDD.partitionBy(hashParts)
        pageRankIterations(partLinks, ranksRDD, numOfIters)
    }

    outputRanks
      .map(ranks => s"${ranks._1}:${ranks.toString}")
      .saveAsTextFile(s"$basePath/page_rank/output/$partStatus")
  }

  /**
   *  Method to compute ranks based on the incoming links,
   *  by iterating over the links.
   *
   *
   * NOTE: In this implementation flatMapValues and mapValues API is
   * used instead of map and flatMap.
   *
   * @param links URLs as key and the list of outgoing links as value.
   * @param ranks Initial rank of each URL.
   * @param numOfIters Number of iterations to converge.
   * @return
   */
  private def pageRankIterations(
    links: RDD[(String, Iterable[String])],
    ranks: RDD[(String, Double)],
    numOfIters: Int
  ): RDD[(String, Double)] = {
    var rankUpdates = ranks
    for (_ <- 0 until numOfIters) {

      val contributions = links
        .join(rankUpdates)
        .flatMapValues({
          case (outLinks, currRank) =>
            val numOfOutLinks = outLinks.size
            outLinks.map(x => (x, currRank / numOfOutLinks))
        })
        .mapValues(_._2)

      rankUpdates = contributions
        .reduceByKey(_ + _)
        .mapValues(_ * 0.85 + 0.15)
    }
    rankUpdates
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
