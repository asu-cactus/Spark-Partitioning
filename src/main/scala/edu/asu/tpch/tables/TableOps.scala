package edu.asu.tpch.tables

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

private[tpch] trait TableOps {

  protected val dateFormat = "yyyy-MM-dd"

  /**
   * Method to get the schema to generate
   * [[DataFrame]] from raw files.
   *
   * @return Schema as [[StructType]] instance.
   */
  protected def getSchema: StructType
  protected def getRawDirName: String
  protected def getParquetDirName: String

  /**
   *  Method to map the raw data to its [[DataFrame]]
   *  schema and create a [[DataFrame]] from raw text files.
   *
   * @param basePath Experimentation directory
   * @param spark [[SparkSession]] application entry point
   * @return [[DataFrame]]
   */
  protected def getRawTableDf(
    basePath: String,
    spark: SparkSession
  ): DataFrame =
    spark.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", "|")
      .schema(getSchema)
      .load(s"$basePath/raw_data/$getRawDirName")

  /**
   *  Method to read raw text files and write the data
   *  to Parquet format.
   *
   * @param basePath Experimentation directory
   * @param spark [[SparkSession]] application entry point
   */
  def rawToParquet(basePath: String)(implicit spark: SparkSession): Unit =
    parquetParts(getRawTableDf(basePath, spark)).write
      .parquet(s"$basePath/parquet/$getParquetDirName")

  /**
   * Method to apply some partitioning before writing
   * the data to disk in Parquet format.
   *
   * @param df [[DataFrame]] of the data
   * @return
   */
  protected def parquetParts(df: DataFrame): DataFrame = df.repartition(80)

  /**
   * Method to read the TPC-H tables which are stored on
   * disk in Parquet format.
   *
   * @param basePath Experimentation directory
   * @param spark [[SparkSession]] application entry point
   * @return [[DataFrame]] of the table
   */
  def readTable(basePath: String)(implicit spark: SparkSession): DataFrame =
    spark.read.parquet(s"$basePath/parquet/$getParquetDirName")

}
