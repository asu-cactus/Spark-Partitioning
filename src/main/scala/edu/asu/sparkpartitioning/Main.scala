package edu.asu.sparkpartitioning

import edu.asu.sparkpartitioning.experiments.{E1, E2, E3}
import edu.asu.sparkpartitioning.utils.MatrixOps.PairedOps
import edu.asu.sparkpartitioning.utils.MatrixPartitioners.IndexedPartitioner
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD

object Main {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      throw new IllegalArgumentException(
        "Base path for storing data, spark history log directory, experiment " +
          "to execute and number of partitions is expected." +
          s"\nProvide: ${args.toList}"
      )
    }
    val basePath = args(0)
    val historyDir = args(1)
    val numOfParts = args(2).toInt
    val experiment = args(4)

    Logger.getLogger("org.spark_project").setLevel(Level.WARN)
    Logger.getLogger("org.apache").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("com").setLevel(Level.WARN)

    implicit val log: Logger = Logger.getLogger("MatrixMultiplication")

    val conf = new SparkConf()
      .setAppName("matrix_multiplication")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.history.fs.logDirectory", historyDir)
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", historyDir)
      .registerKryoClasses(
        Array(
          classOf[RDD[MatrixEntry]],
          classOf[MatrixEntry],
          classOf[PairedOps]
        )
      )

    implicit val sc: SparkContext = new SparkContext(conf)

    val matPartitioner = new IndexedPartitioner(numOfParts)

    experiment match {
      case "e1" => new E1(numOfParts).execute(basePath)
      case "e2" => new E2(numOfParts).execute(basePath, matPartitioner)
      case "e3" => new E3(numOfParts).execute(basePath, matPartitioner)

    }
  }

}
