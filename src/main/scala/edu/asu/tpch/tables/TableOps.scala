package edu.asu.tpch.tables

import com.typesafe.config.Config
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
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
  protected def getTableName: String

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
      .saveAsTable(s"$getTableName" + "_none")

  /**
   *  Method to read raw text files and write the data
   *  to Parquet format.
   *
   * @param basePath Experimentation directory
   * @param spark    [[SparkSession]] application entry point
   * @param configs TypeSafe config object which contains partition
   *                information for the TPC-H tables
   */
  def rawToParquetWithParts(
    basePath: String,
    configs: Config
  )(implicit spark: SparkSession): Unit =
    parquetParts(getRawTableDf(basePath, spark), configs).write
      .saveAsTable(s"$getTableName" + "_parts")

  /**
   *  Method to read raw text files and write the data
   *  to Parquet format.
   *
   * @param basePath Experimentation directory
   * @param spark [[SparkSession]] application entry point
   * @param configs TypeSafe config object which contains partition
   *                information for the TPC-H tables
   */
  def rawToParquetWithBuckets(
    basePath: String,
    configs: Config
  )(implicit spark: SparkSession): Unit =
    parquetBuckets(
      getRawTableDf(basePath, spark),
      configs
    ).mode(SaveMode.Overwrite)
      .saveAsTable(s"$getTableName" + "_buckets")

  /**
   * Method to apply some partitioning before writing
   * the data to disk in Parquet format.
   *
   * @param df [[DataFrame]] of the data
   * @param configs TypeSafe config object which contains partition
   *                information for the TPC-H tables
   * @return
   */
  protected def parquetParts(df: DataFrame, configs: Config): DataFrame =
    if (configs.hasPath(s"$getTableName")) {
      val colName = configs.getString(s"$getTableName.partition_key")
      val numOfParts = configs.getInt(s"$getTableName.num_of_partitions")
      df.repartition(numOfParts, col(colName))
    } else {
      df.repartition()
    }

  /**
   * Method to apply some bucketing before writing
   * the data to disk in Parquet format.
   *
   * @param df [[DataFrameWriter]] of the data
   * @param configs TypeSafe config object which contains partition
   *                information for the TPC-H tables
   * @return
   */
  protected def parquetBuckets(
    df: DataFrame,
    configs: Config
  ): DataFrameWriter[Row] =
    if (configs.hasPath(s"$getTableName")) {
      val colName = configs.getString(s"$getTableName.partition_key")
      val numOfParts = configs.getInt(s"$getTableName.num_of_partitions")
      df.repartition(numOfParts, col(colName))
        .write
        .sortBy(colName)
        .bucketBy(numOfParts, colName)
    } else {
      df.repartition().write
    }

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
    spark.read.table(s"$getTableName" + "_none")

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
    spark.read.table(s"$getTableName" + "_parts")

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
    spark.read.table(s"$getTableName" + "_buckets")

}
