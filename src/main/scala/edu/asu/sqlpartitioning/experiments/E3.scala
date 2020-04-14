package edu.asu.sqlpartitioning.experiments

import edu.asu.sqlpartitioning.utils.ExtraOps.timedBlock
import edu.asu.sqlpartitioning.utils.MatrixOps._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

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
class E3(interNumParts: Int)(implicit ss: SparkSession) {

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

      val leftDF = ss.read.parquet(s"$basePath/common/left.parquet")
      val rightDF = ss.read.parquet(s"$basePath/common/right.parquet")

      leftDF.write.parquet(s"$basePath/e3/left.parquet")
      rightDF.write.parquet(s"$basePath/e3/right.parquet")
    }

    val dataTotalSeconds = timeToDisk / math.pow(10, 3)
    val dataMinutes = (dataTotalSeconds / 60).toLong
    val dataSeconds = (dataTotalSeconds % 60).toInt
    log.info(
      s"E3 -> Time to persist random data to disk is $dataMinutes minutes $dataSeconds seconds"
    )

    val (_, timeToMultiply: Long) = timedBlock {
      val leftDF = ss.read
        .parquet(s"$basePath/e3/left.parquet")
        .repartition(interNumParts, col("columnID"))

      val rightDF = ss.read
        .parquet(s"$basePath/e3/right.parquet")
        .repartition(interNumParts, col("rowID"))

      val res = leftDF.multiply(rightDF, interNumParts)
//      res.show()

      res.write.parquet(s"$basePath/e3/multiplication_op.parquet")
    }

    val multiplyTotalSeconds = timeToMultiply / math.pow(10, 3)
    val multiplyMinutes = (multiplyTotalSeconds / 60).toLong
    val multiplySeconds = (multiplyTotalSeconds % 60).toInt
    log.info(
      s"E3 -> Time to partition, multiply and persist result to " +
        s"disk is $multiplyMinutes minutes $multiplySeconds seconds"
    )
  }

}