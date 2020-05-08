package edu.asu.sparkpartitioning.experiments

import edu.asu.sparkpartitioning.utils.ExtraOps.timedBlock
import edu.asu.sparkpartitioning.utils.MatrixOps._
import org.apache.log4j.Logger
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.storage.StorageLevel

/**
 * This class implements the E1 (as mentioned in the document).
 * Step 1: Partition the matrices
 * Step 2: Write the random matrices to secondary storage
 * Step 3: Read the matrices
 * Step 4: Perform multiplication operation
 * Step 5: Write the result to secondary storage
 *
 * Method [[execute()]] will do all the above mentioned steps.
 * And also calculate time required from step 3 to step 4.
 */
private[sparkpartitioning] class E2(interNumParts: Int)(
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
      val left = sc
        .objectFile[(Int, (Int, Double))](s"$basePath/common/left")
        .partitionBy(matPartitioner)
        .persist(StorageLevel.DISK_ONLY)

      val right = sc
        .objectFile[(Int, (Int, Double))](s"$basePath/common/right")
        .partitionBy(matPartitioner)
        .persist(StorageLevel.DISK_ONLY)

      val dummyCount = left.join(right).count

      left.saveAsObjectFile(s"$basePath/e2/left")
      right.saveAsObjectFile(s"$basePath/e2/right")
    }

    val dataTotalSeconds = timeToDisk / math.pow(10, 3)
    val dataMinutes = (dataTotalSeconds / 60).toLong
    val dataSeconds = (dataTotalSeconds % 60).toInt
    log.info(
      s"E2 -> Time to persist random data to disk after partitioning " +
        s"is $dataMinutes minutes $dataSeconds seconds"
    )

    val (_, timeToMultiply: Long) = timedBlock {
      val leftMat =
        sc.objectFile[(Int, (Int, Double))](s"$basePath/e2/left")
      val rightMat =
        sc.objectFile[(Int, (Int, Double))](s"$basePath/e2/right")

      val res = leftMat.multiply(rightMat, interNumParts)

      res.saveAsObjectFile(s"$basePath/e2/multiplication_op")
    }

    val multiplyTotalSeconds = timeToMultiply / math.pow(10, 3)
    val multiplyMinutes = (multiplyTotalSeconds / 60).toLong
    val multiplySeconds = (multiplyTotalSeconds % 60).toInt
    log.info(
      s"E2 -> Time to multiply and persist result to disk " +
        s"is $multiplyMinutes minutes $multiplySeconds seconds"
    )
  }

}
