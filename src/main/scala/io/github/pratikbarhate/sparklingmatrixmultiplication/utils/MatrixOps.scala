package io.github.pratikbarhate.sparklingmatrixmultiplication.utils

import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD

object MatrixOps {

  /**
   * NOTE: Structure of each element in the RDDs is as expected :-
   * Left -> (Long, (Long, Double)) -> (colId, (rowID, value))
   * Right -> (Long, (Long, Double)) -> (rowID, (colId, value))
   */
  implicit class PairedOps(rdd: RDD[(Long, (Long, Double))]) extends Serializable {
    def multiply(right: RDD[(Long, (Long, Double))], numOfParts: Int): RDD[MatrixEntry] = {

      val joinedMatrices = rdd.join(right, numOfParts)

      joinedMatrices
        .map({ case (_, ((r, lv), (c, rv))) => ((r, c), lv * rv) })
        .reduceByKey(_ + _)
        .map({ case ((r, c), sum) => MatrixEntry(r, c, sum) })
    }
  }

}
