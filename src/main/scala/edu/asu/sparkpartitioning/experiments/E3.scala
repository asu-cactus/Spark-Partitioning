package edu.asu.sparkpartitioning.experiments

import edu.asu.utils.ExtraOps.timedBlock
import edu.asu.sparkpartitioning.utils.MatrixOps._
import org.apache.log4j.Logger
import org.apache.spark.{Partitioner, SparkContext}

/**
 * This class implements the E1 (as mentioned in the document).
 * Step 1: Write the random matrices to secondary storage
 * Step 2: Read the matrices
 * Step 3: Partition the matrices
 * Step 4: Perform multiplication operation
 * Step 5: Write the result to secondary storage
 *
 * Method [[execute()]] will do all the above mentioned steps.
 * And also calculate time required from step 3 to step 4.
 */
private[sparkpartitioning] class E3(interNumParts: Int)(
  implicit sc: SparkContext
) {

  /**
   * Method to execute the required steps.
   *
   * @param basePath Base path on the secondary storage
   * @param log      Logger instance to display result on the console.
   */
  def execute(basePath: String, matPartitioner: Partitioner)(
    implicit log: Logger
  ): Unit = {

    val (_, timeToDisk: Long) = timedBlock {
      val left = sc.objectFile[(Int, (Int, Double))](s"$basePath/common/left")
      val right =
        sc.objectFile[(Int, (Int, Double))](s"$basePath/common/right")

      left.saveAsObjectFile(s"$basePath/e3/left")
      right.saveAsObjectFile(s"$basePath/e3/right")
    }

    val dataTotalSeconds = timeToDisk / math.pow(10, 3)
    log.info(
      s"E3 -> Time to persist random data to disk is $dataTotalSeconds seconds"
    )

    val (_, timeToMultiply: Long) = timedBlock {
      val leftMat = sc
        .objectFile[(Int, (Int, Double))](s"$basePath/e3/left")
        .partitionBy(matPartitioner)

      val rightMat = sc
        .objectFile[(Int, (Int, Double))](s"$basePath/e3/right")
        .partitionBy(matPartitioner)

      val res = leftMat.multiply(rightMat, interNumParts)

      res.count
    }

    val multiplyTotalSeconds = timeToMultiply / math.pow(10, 3)
    log.info(
      s"E3 -> Time to partition, multiply and persist result to " +
        s"disk is $multiplyTotalSeconds seconds"
    )
  }

}
