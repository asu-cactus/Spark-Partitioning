package edu.asu.sparkpartitioning.utils

import edu.asu.sparkpartitioning.utils.Parser.MatrixEntry
import org.apache.spark.rdd.RDD

object MatrixOps {

  /**
   * NOTE: Structure of each element in the RDDs is as expected :-
   * Left -> (Int, (Int, Double)) -> (colId, (rowID, value))
   * Right -> (Int, (Int, Double)) -> (rowID, (colId, value))
   */
  implicit class PairedOps(rdd: RDD[(Int, (Int, Double))])
      extends Serializable {
    def multiply(
      right: RDD[(Int, (Int, Double))],
      numOfParts: Int
    ): RDD[MatrixEntry] = {

      val joinedMatrices = rdd.join(right)

      joinedMatrices
        .map({ case (_, ((r, lv), (c, rv))) => ((r, c), lv * rv) })
        .reduceByKey(_ + _)
        .map({ case ((r, c), sum) => MatrixEntry(r, c, sum) })
    }
  }

}
