package edu.asu.hyperspacetest.experiments

import edu.asu.utils.ExtraOps.timedBlock
import edu.asu.utils.MatrixOps._
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This class implements the E1 (as mentioned in the document).
 * Step 1: Write the random matrices to secondary storage
 * Step 2: Read the matrices
 * Step 3: Perform multiplication operation
 * Step 4: Write the result to secondary storage
 *
 * Method [[execute()]] will do all the above mentioned steps.
 * And also calculate time required from step 2 to step 4.
 *
 * @param spark [[SparkSession]] of the application.
 */
private[hyperspacetest] class E1(interNumParts: Int)(
  implicit spark: SparkSession
) {

  /**
   * Method to execute the required steps.
   *
   * @param basePath Base path on the secondary storage
   * @param log      Logger instance to display result on the console.
   */
  def execute(basePath: String)(implicit log: Logger): Unit = {

    val (_, timeToMultiply: Long) = timedBlock {
      val leftDF = spark.read.parquet(s"$basePath/common/left")
      val rightDF = spark.read.parquet(s"$basePath/common/right")

      val res: DataFrame = leftDF.multiply(rightDF, interNumParts)

      res.count
    }

    val multiplyTotalSeconds = timeToMultiply / math.pow(10, 3)
    log.info(
      s"E1 -> Time to multiply and persist result to disk " +
        s"is $multiplyTotalSeconds seconds"
    )
  }

}
