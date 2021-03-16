package edu.asu.hyperspacetest.experiments

import edu.asu.utils.ExtraOps.timedBlock
import edu.asu.utils.MatrixOps._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import com.microsoft.hyperspace._
import com.microsoft.hyperspace.index.IndexConfig

private[hyperspacetest] class E2(interNumParts: Int)(
  implicit spark: SparkSession
) {

  /**
   * Method to execute the required steps.
   *
   * @param basePath Base path on the secondary storage
   * @param log      Logger instance to display result on the console.
   */
  def execute(basePath: String)(
    implicit log: Logger
  ): Unit = {

    spark.enableHyperspace()

    val (_, timeToDisk: Long) = timedBlock {

      val hyperspace: Hyperspace = Hyperspace()

      val leftDF = spark.read.parquet(s"$basePath/common/left")
      val rightDF = spark.read.parquet(s"$basePath/common/right")

      val leftIndexConfig =
        IndexConfig("leftMatrix", Seq("columnID"), Seq("rowID"))
      val rightIndexConfig =
        IndexConfig("rightMatrix", Seq("rowID"), Seq("columnID"))

      hyperspace.createIndex(leftDF, leftIndexConfig)
      hyperspace.createIndex(rightDF, rightIndexConfig)
    }

    val dataTotalSeconds = timeToDisk / math.pow(10, 3)
    log.info(
      s"E2 -> Time to create Hyperspace indexes " +
        s"is $dataTotalSeconds seconds"
    )

    val (_, timeToMultiply: Long) = timedBlock {

      val leftDF = spark.read.parquet(s"$basePath/common/left")
      val rightDF = spark.read.parquet(s"$basePath/common/right")

      val res = leftDF.multiply(rightDF, interNumParts)

      res.count
    }

    val multiplyTotalSeconds = timeToMultiply / math.pow(10, 3)
    log.info(
      s"E2 -> Time to multiply " +
        s"is $multiplyTotalSeconds seconds"
    )
  }

}
