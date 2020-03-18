package io.github.pratikbarhate.sparklingmatrixmultiplication.utils

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

object Generator {

  /**
   * Method to generate a random [[RDD]] of [[MatrixEntry]] type,
   * with given number of rows and columns
   *
   * @param rowCount Number of rows
   * @param colCount Number of columns
   * @param sc       [[SparkContext]] used by the application
   * @return [[RDD]] of [[MatrixEntry]]
   */
  // TODO: This method is too slow.
  // FIXME: This is not to be used.
  @deprecated
  def getRandomMatrixRdd(rowCount: Long, colCount: Long, numPartitions: Int)
                        (implicit sc: SparkContext): RDD[MatrixEntry] = {
    val rand = scala.util.Random
    var initRDD = sc.parallelize(Seq.empty[MatrixEntry], colCount.toInt)
    for (r <- 0L until rowCount) {
      for (c <- 0L until colCount) {
        val currElement = sc.parallelize(Seq(MatrixEntry(r, c, rand.nextDouble())), 1)
        initRDD = initRDD ++ currElement
      }
    }
    initRDD
  }


  //FIXME: There are multiple issues in the random generator.

  //  def getRandomMatrixRdd(rowCount: Long, colCount: Long, numPartitions: Int)
  //                        (implicit sc: SparkContext): RDD[MatrixEntry] = {
  //    val rowCounter = sc
  //      .collectionAccumulator[collection.mutable.Map[Long, Long]](s"Row Counters")
  //    (0L until colCount)
  //      .map(colId => {
  //        val temp = randomRDD(sc, new MatrixCellGenerator(rowCount, colId, rowCounter),
  //          rowCount, numPartitions)
  //        temp
  //      })
  //      .reduce(_ ++ _)
  //  }

  /**
   * Method to generate a random  [[BlockMatrix]].
   *
   * @param rowCount Number of rows
   * @param colCount Number of columns
   * @param sc       [[SparkContext]] used by the application
   * @return [[RDD]] of [[MatrixEntry]]
   */
  @deprecated
  def getRandomBlockMatrix(rowCount: Long, colCount: Long, numPartitions: Int,
                           rowsPerBlock: Int, colsPerBlock: Int)
                          (implicit sc: SparkContext): BlockMatrix = {
    new CoordinateMatrix(
      getRandomMatrixRdd(rowCount, colCount, numPartitions)(sc)
    ).toBlockMatrix(rowsPerBlock, colsPerBlock)
  }

  /**
   * Random data generator for matrix of row x col.
   * It generates values for each cell at each iteration.
   *
   * NOTE: We are following the convention that the
   * member 'i' of the [[MatrixEntry]] represents row number,
   * and member 'j' represents column number.
   *
   * @param rows  Number of rows
   * @param colId Number of columns
   */
  //  private class MatrixCellGenerator(rows: Long, colId: Long,
  //                                    rowCounter: CollectionAccumulator[collection.mutable.Map[Long, Long]])
  //    extends RandomDataGenerator[MatrixEntry] {
  //    private val rand: Random.type = scala.util.Random
  //
  //    override def nextValue(): MatrixEntry = {
  //      if (rowCounter >= rows) {
  //        throw new IndexOutOfBoundsException(s"Number of rows generated are more than specified")
  //      }
  //      val cellValue = MatrixEntry(rowCounter, colId, rand.nextInt())
  //      rowCounter += 1L
  //      cellValue
  //    }
  //
  //    override def copy(): RandomDataGenerator[MatrixEntry] = new MatrixCellGenerator(rows, colId, rowCounter)
  //
  //    override def setSeed(seed: Long): Unit = rand.setSeed(seed)
  //  }

}
