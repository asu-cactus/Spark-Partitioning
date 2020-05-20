package edu.asu.tpch.tables

import org.apache.spark.sql._
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
    transformRawDf(
      spark.read
        .format("csv")
        .option("header", "false")
        .option("delimiter", "|")
        .schema(getSchema)
        .load(s"$basePath/raw_data/$getRawDirName")
    )

  /**
   * Method to process raw data read from text files.
   *
   * @param df [[DataFrame]] generated from text files.
   * @return
   */
  protected def transformRawDf(df: DataFrame): DataFrame = df

  /**
   *  Method to read raw text files and write the data
   *  to Parquet format.
   *
   * @param basePath Experimentation directory
   * @param spark [[SparkSession]] application entry point
   */
  def rawToParquet(basePath: String)(implicit spark: SparkSession): Unit =
    getRawTableDf(basePath, spark).write
      .saveAsTable(s"$getParquetDirName" + "_none")

  /**
   *  Method to read raw text files and write the data
   *  to Parquet format.
   *
   * @param basePath Experimentation directory
   * @param spark [[SparkSession]] application entry point
   */
  def rawToParquetWithParts(
    basePath: String,
    numOfParts: Int
  )(implicit spark: SparkSession): Unit =
    parquetParts(getRawTableDf(basePath, spark), numOfParts).write
      .saveAsTable(s"$getParquetDirName" + "_parts")

  /**
   *  Method to read raw text files and write the data
   *  to Parquet format.
   *
   * @param basePath Experimentation directory
   * @param spark [[SparkSession]] application entry point
   */
  def rawToParquetWithBuckets(
    basePath: String,
    numOfParts: Int
  )(implicit spark: SparkSession): Unit =
    parquetBuckets(
      getRawTableDf(basePath, spark),
      numOfParts
    ).mode(SaveMode.Overwrite)
      .saveAsTable(s"$getParquetDirName" + "_buckets")

  /**
   * Method to apply some partitioning before writing
   * the data to disk in Parquet format.
   *
   * @param df [[DataFrame]] of the data
   * @return
   */
  protected def parquetParts(df: DataFrame, numOfParts: Int): DataFrame =
    df.repartition(numOfParts)

  /**
   * Method to apply some bucketing before writing
   * the data to disk in Parquet format.
   *
   * @param df [[DataFrameWriter]] of the data
   * @return
   */
  protected def parquetBuckets(
    df: DataFrame,
    numOfParts: Int
  ): DataFrameWriter[Row] =
    df.repartition(numOfParts).write

  /**
   * Method to read the TPC-H tables which are stored on
   * disk in Parquet format.
   *
   * @param basePath Experimentation directory
   * @param spark [[SparkSession]] application entry point
   * @return [[DataFrame]] of the table
   */
  def readTable(
    basePath: String
  )(implicit spark: SparkSession): DataFrame =
    spark.read.table(s"$getParquetDirName" + "_none")

  /**
   * Method to read the TPC-H tables which are stored on
   * disk in Parquet format after repartitioning.
   *
   * @param basePath Experimentation directory
   * @param spark [[SparkSession]] application entry point
   * @return [[DataFrame]] of the table
   */
  def readTableFromParts(
    basePath: String
  )(implicit spark: SparkSession): DataFrame =
    spark.read.table(s"$getParquetDirName" + "_parts")

  /**
   * Method to read the TPC-H tables which are stored on
   * disk in Parquet format after Bucketing.
   *
   * @param basePath Experimentation directory
   * @param spark [[SparkSession]] application entry point
   * @return [[DataFrame]] of the table
   */
  def readTableFromBuckets(
    basePath: String
  )(implicit spark: SparkSession): DataFrame =
    spark.read.table(s"$getParquetDirName" + "_buckets")

}
