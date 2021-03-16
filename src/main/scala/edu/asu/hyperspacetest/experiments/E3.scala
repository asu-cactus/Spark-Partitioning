package edu.asu.hyperspacetest.experiments
import edu.asu.utils.ExtraOps.timedBlock
import edu.asu.utils.MatrixOps._
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

private[hyperspacetest] class E3(interNumParts: Int)(
  implicit spark: SparkSession
) {

  def execute(basePath: String)(implicit log: Logger): Unit = {

    val (_, timeToMultiply: Long) = timedBlock {
      val leftDF = spark.read.parquet(s"$basePath/common/left").cache
      val rightDF = spark.read.parquet(s"$basePath/common/right").cache
      leftDF.count
      rightDF.count

      val res: DataFrame = leftDF.multiply(rightDF, interNumParts)

      res.count
    }

    val multiplyTotalSeconds = timeToMultiply / math.pow(10, 3)
    log.info(
      s"E3 -> Time to multiply " +
        s"is $multiplyTotalSeconds seconds"
    )
  }

}
