package edu.asu.sqlpartitioning.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object MatrixOps {

  /**
   * NOTE: Structure of each row in the DataFrames is as expected :-
   * Left -> (Int, Int, Double)) -> (rowID, colID, value)
   * Right -> (Int, Int, Double) -> (rowID, colId, value)
   */
  implicit class PairedOps(left: DataFrame) extends Serializable {
    def multiply(
      right: DataFrame,
      numOfParts: Int
    ): DataFrame = {

      val joinedDF = left
        .as("LEFT")
        .join(right.as("RIGHT"), col("LEFT.columnID") === col("RIGHT.rowID"))

      joinedDF
        .drop("LEFT.columnID", "RIGHT.rowID")
        .withColumn("m_op", col("LEFT.value") * col("RIGHT.value"))
        .groupBy("LEFT.rowID", "RIGHT.columnID")
        .agg(sum("m_op").as("value"))
        .withColumnRenamed("LEFT.rowID", "rowID")
        .withColumnRenamed("RIGHT.columnID", "columnID")

    }
  }

}
