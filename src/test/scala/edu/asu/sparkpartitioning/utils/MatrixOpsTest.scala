package edu.asu.sparkpartitioning.utils

import edu.asu.sparkpartitioning.TestBase
import io.github.pratikbarhate.sparklingmatrixmultiplication.TestBase
import io.github.pratikbarhate.sparklingmatrixmultiplication.utils.MatrixOps._
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD

class MatrixOpsTest extends TestBase {

  private val l0: RDD[MatrixEntry] = sc.parallelize(
    List(
      MatrixEntry(0, 0, 1),
      MatrixEntry(0, 1, 2),
      MatrixEntry(0, 2, 3),
      MatrixEntry(1, 0, 9),
      MatrixEntry(1, 1, 7),
      MatrixEntry(1, 2, 6)
    )
  )

  private val r0: RDD[MatrixEntry] = sc.parallelize(
    List(
      MatrixEntry(0, 0, 10),
      MatrixEntry(0, 1, 20),
      MatrixEntry(1, 0, 30),
      MatrixEntry(1, 1, 40),
      MatrixEntry(2, 0, 50),
      MatrixEntry(2, 1, 60)
    )
  )

  private val e0 = sc.parallelize(
    List(
      MatrixEntry(0, 0, 220),
      MatrixEntry(0, 1, 280),
      MatrixEntry(1, 0, 600),
      MatrixEntry(1, 1, 820)
    )
  )

  private val pl0 = l0.map({ case MatrixEntry(i, j, value) => (j, (i, value)) })
  private val pr0 = r0.map({ case MatrixEntry(i, j, value) => (i, (j, value)) })

  "Multiplication of pl0 and pr0" should "return e0 as the result" in {
    val t0 = pl0.multiply(pr0, numOfParts = 3)
    val res = t0.subtract(e0)
    assert(
      res.count() == 0,
      "The paired matrix multiplication result is not as expected."
    )
  }

}
