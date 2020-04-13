### Spark-Partitioning

Investigating and benchmarking how partitioning of data on HDFS will affect Spark performance

#### Steps to execute the experiments based on RDD.

1. Build using the command - `mvn clean package`. It will create a tar.gz file named `Spark-Partitioning-0.1-SNAPSHOT.tar.gz` with the 4 directories `bin`, `etc`, `python` and `lib`.
Copy the `tar.gz` file to the cluster and decompress the folder.

2. To create random matrices and load them to HDFS, execute the shell script `nohup ./bin/load_data.sh ${ROW_LEFT} {COL_LEFT} ${ROW_RIGHT} ${COL_RIGHT} ${BASE_PATH} > load_data.logs &`.

3. To execute a particular experiment, execute the shell script `nohup ./bin/run_experiment.sh ${BASE_PATH} ${EXPERIMENT} ${NUM_OF_PARTITIONS} > job_${EXPERIMENT}_${PARTITIONS}.logs &`.
Allowed values for `${EXPERIMENT}` are `e1`, `e2` or `e3`.

4. To execute naive implementation of `PageRank` (normal code and with co-partitioning both) on Spark, execute command
  `nohup ./bin/run_page_rank.sh ${NUM_OF_PAGES} ${MAX_LINKS} ${RAW_DATA_OP_NM} ${BASE_PATH} > rank.logs &`.

#### Steps to execute TPC-H benchmarks.

1. Load raw TPC-H data, generated from `dbgen` to a location `${BASE_PATH}/raw_data/`.

2. Convert the raw files into parquet data by running the command `./bin/convert_tpch.sh ${BASE_PATH}`.

**Code style notes**
1. Python indentation and tabs = 4 spaces. (We are using Python 3)
2. Bash script indentation and tabs = 2 spaces.
3. Set up the Scalafmt plugin and use the `.scalafmt.conf` for auto formatting.