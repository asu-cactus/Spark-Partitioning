package edu.asu.sparkpartitioning.experiments

import edu.asu.sparkpartitioning.utils.ExtraOps.timedBlock
import edu.asu.sparkpartitioning.utils.MatrixOps._
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

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
 * @param sc [[SparkContext]] of the application.
 */
private[sparkpartitioning] class E1(interNumParts: Int)(
  implicit sc: SparkContext
) {

  /**
   * Method to execute the required steps.
   *
   * @param basePath Base path on the secondary storage
   * @param log      Logger instance to display result on the console.
   */
  def execute(basePath: String)(implicit log: Logger): Unit = {

    val (_, timeToDisk: Long) = timedBlock {
      val left =
        sc.objectFile[(Int, (Int, Double))](s"$basePath/common/left")
      val right =
        sc.objectFile[(Int, (Int, Double))](s"$basePath/common/right")

      left.saveAsObjectFile(s"$basePath/e1/left")
      right.saveAsObjectFile(s"$basePath/e1/right")
    }

    val dataTotalSeconds = timeToDisk / math.pow(10, 3)
    log.info(
      s"E1 -> Time to persist random data to disk is $dataTotalSeconds seconds"
    )

    val (_, timeToMultiply: Long) = timedBlock {
      val leftMat =
        sc.objectFile[(Int, (Int, Double))](s"$basePath/e1/left")
      val rightMat =
        sc.objectFile[(Int, (Int, Double))](s"$basePath/e1/right")

      val res = leftMat.multiply(rightMat, interNumParts)

      res.count
    }

    val multiplyTotalSeconds = timeToMultiply / math.pow(10, 3)
    log.info(
      s"E1 -> Time to multiply and persist result to disk " +
        s"is $multiplyTotalSeconds seconds"
    )
  }

}
