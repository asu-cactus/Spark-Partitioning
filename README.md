### Spark-Partitioning

Investigating and benchmarking how partitioning of data on secondary storage layer will affect Spark performance

#### Environment Variables

Configure and update the following variable in your systems `bashrc` or `bash_profile` 
or `zprofile` or `profile` depending on which operating system you are using. 
`SPARK_DEFAULT_PAR` is used to set a value to `spark.default.parallelism` configuration 
while running the spark applications through the provided shell scripts.

        # JAVA configuration
        export JAVA_HOM="/usr/lib/jvm/java-8-openjdk-amd64"
        
        # Hadoop configuration
        export HADOOP_HOME="/home/ubuntu/hadoop"
        PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
        
        # Apache Spark configuration
        export SPARK_HOME="/home/ubuntu/spark"
        PATH=${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
        
        # Apache Hive configurations
        export HIVE_HOME="/home/ubuntu/hive"
        PATH=${PATH}:${HIVE_HOME}/bin
        export HIVE_CONF_DIR="${HIVE_HOME}/conf"
        
        # Installations 
        export HADOOP_MASTER="${MASTER_NODE_IP}:9000"
        export SPARK_MASTER="${MASTER_NODE_IP}:7077"
        export SPARK_DEFAULT_PAR="16"

#### Steps to execute the experiments based on matrix multiplication.

1. Build using the command - `mvn clean package`. It will create a tar.gz file 
named `Spark-Partitioning-0.1-SNAPSHOT.tar.gz` with the 4 directories `bin`, `etc`, `python` and `lib`.
Copy the `tar.gz` file to the cluster and decompress the folder.

2. To create random matrices and load them to HDFS, execute the shell script 
`nohup ./bin/load_data.sh ${ROW_LEFT} {COL_LEFT} ${ROW_RIGHT} ${COL_RIGHT} ${WORK_FLOW = (RDD, SQL)} ${BASE_PATH} ${NUM_OF_PARTS} > logs/load_data.log &`.

3. To execute a particular experiment, execute the shell script 
`nohup ./bin/run_experiment.sh ${WORK_FLOW = (RDD, SQL, BUCKET, HIVE, INDEXED)} ${BASE_PATH} ${EXPERIMENT} ${NUM_OF_PARTITIONS} > logs/job_${EXPERIMENT}_${PARTITIONS}.log &`.
Allowed values for `${EXPERIMENT}` are `e1`, `e2` or `e3`. NOTE: With `BUCKET` type workload `e3` experiment is not valid.

#### Steps to execute the experiment for Page Rank.

* To execute naive implementation of `PageRank` on Spark, execute command
 `nohup ./bin/run_page_rank.sh ${NUM_OF_PAGES} ${MAX_LINKS} ${RAW_DATA_OP_NM} ${BASE_PATH} ${NUM_OF_ITERATIONS} > logs/rank.log &`.

#### Steps to execute TPC-H benchmarks.

1. Load raw TPC-H data, generated from `dbgen` to a location `${BASE_PATH}/raw_data/`.

2. Convert the raw files into parquet data by running the command 
`nohup ./bin/convert_tpch.sh ${BASE_PATH} > logs/tpch_data.log &`.

3. To execute TPC-H query use command 
`nohup ./bin/run_tpch_query.sh ${BASE_PATH} ${QUERY_NUM} ${PARTITION_TYPE} {NUM_OF_PARTS} > logs/query_${QUERY_NUM}_${PARTITION_TYPE}.log &`. 
If you want to run all the queries use `${QUERY_NUM}=all`, and for custom query defined 
in `Custom` class use `${QUERY_NUM}=custom`. Allowed values for `${PARTITION_TYPE}` 
are `hyperspace`, `parts` and `buckets`. You are required to clear the hyperspace indexes, if there are any with same names previously created.
`{NUM_OF_PARTS}` is used to set the value for the spark configuration `spark.sql.shuffle.partitions`.

* _NOTE_: Currently the option to execute with `buckets` (`hive`) is commented as the current testing
scenarios create large number of buckets which is undesirable and performs worse.

4. Partitioning and bucketing keys are configurable by editing the `tpch.conf` file available
in the `etc` directory within the package or in `src/resources/configurations` within the source code.
Spark SQL `DataFrames` have case sensitive column names, all the column names are in `uppercase`.
 
#### Overhead analysis

1. To execute super graph module use command -
`nohup ./Spark-Partitioning-0.1-SNAPSHOT/bin/super_graph.sh ${GRAPH_COUNT} ${BATCH_SIZE} > logs/super_graph.log &`

2. To execute the graph matching module use command - 
`nohup ./Spark-Partitioning-0.1-SNAPSHOT/bin/graph_matching.sh ${TOTAL_NUM_TREES} ${AVG_NUM_NODES} ${LOW_BOUND_NODES} ${NUM_OF_THREADS} ${BATCH_SIZE} > logs/qraph_matching.log &`

**Code style notes**
1. Python indentation and tabs = 4 spaces. (We are using Python 3)
2. Bash script indentation and tabs = 2 spaces.
3. Set up the Scalafmt plugin and use the `.scalafmt.conf` for auto formatting scala code.