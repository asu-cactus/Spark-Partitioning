package edu.asu.tpch.tables

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConversions._

import com.typesafe.config.Config
import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.IndexConfig

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
  def rawToParquetHyperspace(
    basePath: String,
    configs: Config,
    hyperspace: Hyperspace
  )(
    implicit spark: SparkSession
  ): Unit = {
    val dfWriter = if (configs.hasPath(s"$getTableName")) {
      val numOfParts = configs.getInt(s"$getTableName.num_of_partitions")
      getRawTableDf(basePath, spark).repartition(numOfParts).write
    } else { getRawTableDf(basePath, spark).write }

    dfWriter.parquet(s"$basePath/tables/$getTableName" + "_hyperspace")

    // Create the hyperspace index on the written data.
    // Reference to the data location needs to be the same.
    if (configs.hasPath(s"$getTableName")) {
      val refDf = spark.read
        .parquet(s"$basePath/tables/$getTableName" + "_hyperspace")
      val colNames = configs.getStringList(s"$getTableName.partition_keys")
      val proCols = configs.getStringList(s"$getTableName.projection_keys")
      hyperspace.deleteIndex(s"$getTableName")
      hyperspace.vacuumIndex(s"$getTableName")
      hyperspace.createIndex(
        refDf,
        IndexConfig(s"$getTableName", colNames, proCols)
      )
    }
  }

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
  )(implicit spark: SparkSession): Unit = {
    val rawDf = getRawTableDf(basePath, spark)

    val df = if (configs.hasPath(s"$getTableName")) {
      val numOfParts = configs.getInt(s"$getTableName.num_of_partitions")
      rawDf.repartition(numOfParts)
    } else { rawDf.repartition() }

    df.write.parquet(s"$basePath/tables/$getTableName" + "_parts")
  }

  /**
   *  Method to read raw text files and write the data
   *  to Parquet format.
   *
   * @param basePath Experimentation directory
   * @param spark [[SparkSession]] application entry point
   * @param configs TypeSafe config object which contains partition
   *                information for the TPC-H tables
   */
  def rawToTableWithBuckets(
    basePath: String,
    configs: Config
  )(implicit spark: SparkSession): Unit = {
    val rawDf = getRawTableDf(basePath, spark)

    val dfWriter = if (configs.hasPath(s"$getTableName")) {
      val colName = configs.getStringList(s"$getTableName.partition_keys").head
      val numOfParts = configs.getInt(s"$getTableName.num_of_partitions")
      rawDf
        .repartition()
        .write
        .bucketBy(numOfParts, colName)
        .sortBy(colName)
    } else { rawDf.repartition().write }

    dfWriter
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"$getTableName" + "_buckets")
  }

  /**
   * Method to read the TPC-H tables which are stored on
   * disk in Parquet format.
   *
   * @param basePath Experimentation directory
   * @param spark [[SparkSession]] application entry point
   * @return [[DataFrame]] of the table
   */
  def readTableHyperspace(
    basePath: String
  )(implicit spark: SparkSession): DataFrame =
    spark.read.parquet(s"$basePath/tables/$getTableName" + "_hyperspace")

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
    spark.read.parquet(s"$basePath/tables/$getTableName" + "_parts")

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
