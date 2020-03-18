package io.github.pratikbarhate.sparklingmatrixmultiplication.utils

import io.github.pratikbarhate.sparklingmatrixmultiplication.TestBase
import io.github.pratikbarhate.sparklingmatrixmultiplication.utils.Generator.getRandomMatrixRdd
import org.apache.spark.mllib.linalg.distributed.MatrixEntry

class GeneratorTest extends TestBase {

  "The random matrix generator" should "return [[RDD]] of [[MatrixEntry]] of size 2x2" in {
    val genRDD = getRandomMatrixRdd(2, 2, numPartitions = 1)(sc)
    val listOfCellIndexes = genRDD.map({ case MatrixEntry(i, j, value) => (i, j) }).collect.toSet
    val expected = Set((0L, 0L), (0L, 1L), (1L, 0L), (1L, 1L))
    assert(expected.intersect(listOfCellIndexes).size == expected.size)
  }

  "The random matrix generator" should "return [[RDD]] of [[MatrixEntry]] of size 4x4" in {
    val genRDD = getRandomMatrixRdd(4, 4, numPartitions = 1)(sc)
    val listOfCellIndexes = genRDD.map({ case MatrixEntry(i, j, value) => (i, j) }).collect.toSet
    val expected = Set(
      (0L, 0L), (0L, 1L), (0L, 2L), (0L, 3L),
      (1L, 0L), (1L, 1L), (1L, 2L), (1L, 3L),
      (2L, 0L), (2L, 1L), (2L, 2L), (2L, 3L),
      (3L, 0L), (3L, 1L), (3L, 2L), (3L, 3L)
    )
    assert(expected.intersect(listOfCellIndexes).size == expected.size)
  }

  "The random matrix generator" should "return [[RDD]] of [[MatrixEntry]] of size 1x3" in {
    val genRDD = getRandomMatrixRdd(1, 3, numPartitions = 1)(sc)
    val listOfCellIndexes = genRDD.map({ case MatrixEntry(i, j, value) => (i, j) }).collect.toSet
    val expected = Set((0L, 0L), (0L, 1L), (0L, 2L))
    assert(expected.intersect(listOfCellIndexes).size == expected.size)
  }

  "The random matrix generator" should "return [[RDD]] of [[MatrixEntry]] of size 3x1" in {
    val genRDD = getRandomMatrixRdd(3, 1, numPartitions = 1)(sc)
    val listOfCellIndexes = genRDD.map({ case MatrixEntry(i, j, value) => (i, j) }).collect.toSet
    val expected = Set((0L, 0L), (1L, 0L), (2L, 0L))
    assert(expected.intersect(listOfCellIndexes).size == expected.size)
  }

}
