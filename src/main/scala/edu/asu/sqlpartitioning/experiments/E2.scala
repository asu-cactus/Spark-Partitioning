package edu.asu.sqlpartitioning.experiments

import edu.asu.sqlpartitioning.utils.ExtraOps.timedBlock
import org.apache.log4j.Logger
import edu.asu.sqlpartitioning.utils.MatrixOps._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
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
class E2(interNumParts: Int)(implicit ss: SparkSession) {

  /**
   * Method to execute the required steps.
   *
   * @param basePath Base path on the secondary storage
   * @param log      Logger instance to display result on the console.
   */
  def execute(basePath: String)(
    implicit log: Logger
  ): Unit = {

    val (_, timeToDisk: Long) = timedBlock {
      val leftDF = ss.read
        .parquet(s"$basePath/common/left.parquet")
        .repartition(interNumParts, col("columnID"))
        .persist(StorageLevel.DISK_ONLY)

      val rightDF = ss.read
        .parquet(s"$basePath/common/right.parquet")
        .repartition(interNumParts, col("rowID"))
        .persist(StorageLevel.DISK_ONLY)

      val dummyCount = leftDF.join(rightDF).count

      leftDF.write.parquet(s"$basePath/e2/left.parquet")
      rightDF.write.parquet(s"$basePath/e2/right.parquet")
    }

    val dataTotalSeconds = timeToDisk / math.pow(10, 3)
    val dataMinutes = (dataTotalSeconds / 60).toLong
    val dataSeconds = (dataTotalSeconds % 60).toInt
    log.info(
      s"E2 -> Time to persist random data to disk after partitioning " +
        s"is $dataMinutes minutes $dataSeconds seconds"
    )

    val (_, timeToMultiply: Long) = timedBlock {
      val leftDF = ss.read.parquet(s"$basePath/e2/left.parquet")
      val rightDF = ss.read.parquet(s"$basePath/e2/right.parquet")

      val res = leftDF.multiply(rightDF, interNumParts)
//      res.show()

      res.write.parquet(s"$basePath/e2/matrix_op.parquet")
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
