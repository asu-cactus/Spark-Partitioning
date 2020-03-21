package edu.asu.sparkpartitioning

import edu.asu.sparkpartitioning.utils.Parser.readMatrix
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD

object TextToObjectFiles {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      throw new IllegalArgumentException(
        "Base path for storing data and spark history log directory are expected." +
          s"\nProvide: ${args.toList}"
      )
    }
    val basePath = args(0)
    val historyDir = args(1)

    Logger.getLogger("org.spark_project").setLevel(Level.WARN)
    Logger.getLogger("org.apache").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("com").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setAppName("parsing_text_to_object_files")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.history.fs.logDirectory", historyDir)
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", historyDir)

    implicit val sc: SparkContext = new SparkContext(conf)

    val left: RDD[(Long, (Long, Double))] = readMatrix(s"$basePath/raw/left")
      .map({ case MatrixEntry(i, j, value) => (j, (i, value)) })
    val right: RDD[(Long, (Long, Double))] = readMatrix(s"$basePath/raw/right")
      .map({ case MatrixEntry(i, j, value) => (i, (j, value)) })

    left.saveAsObjectFile(s"$basePath/common/left")
    right.saveAsObjectFile(s"$basePath/common/right")
  }

}
