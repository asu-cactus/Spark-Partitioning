package io.github.pratikbarhate.sparklingmatrixmultiplication

import io.github.pratikbarhate.sparklingmatrixmultiplication.experiments._
import io.github.pratikbarhate.sparklingmatrixmultiplication.utils.MatrixOps.PairedOps
import io.github.pratikbarhate.sparklingmatrixmultiplication.utils.MatrixPartitioners.IndexedPartitioner
import io.github.pratikbarhate.sparklingmatrixmultiplication.utils.Parser.readMatrix
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      throw new IllegalArgumentException(
        "Base path for storing data, spark history log directory and number of parts are expected." +
          s"\nProvide: ${args.toList}"
      )
    }
    val basePath = args(0)
    val historyDir = args(1)
    val numOfParts = args(2).toInt

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
      .registerKryoClasses(Array(
        classOf[RDD[MatrixEntry]],
        classOf[MatrixEntry],
        classOf[PairedOps]
      ))

    implicit val sc: SparkContext = new SparkContext(conf)

    val matPartitioner = new IndexedPartitioner(numOfParts)

    val left = readMatrix(s"$basePath/raw/left")
      .map({ case MatrixEntry(i, j, value) => (j, (i, value)) })
    val right = readMatrix(s"$basePath/raw/right")
      .map({ case MatrixEntry(i, j, value) => (i, (j, value)) })

    left.saveAsObjectFile(s"$basePath/common/left")
    right.saveAsObjectFile(s"$basePath/common/right")

    new E1(numOfParts).execute(basePath)
    new E2(numOfParts).execute(basePath, matPartitioner)
    new E3(numOfParts).execute(basePath, matPartitioner)
  }
}
