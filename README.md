## Spark-Partitioning

Investigating and benchmarking how partitioning of data on HDFS will affect Spark performance

##### Steps to execute the experiments.

// TODO: Update the steps

1. Build using the command - `mvn clean package`. It will create a tar.gz file named `SparklingMatrixMultiplication-0.1-SNAPSHOT.tar.gz` with the 4 directories `bin`, `etc`, `python` and `lib`. 

2. Generate random matrices using python script, and copy the generated matrices to a location `${BASE_PATH}/raw/left/` and `${BASE_PATH}/raw/right/` respectively.

3. Execute the Spark program using the below command
    ```
    nohup spark-submit \
    --class io.github.pratikbarhate.sparklingmatrixmultiplication.Main \
    --master spark://${MASTER_PRIVATE_IP}:7077 \
    --deploy-mode client \
    jars/SparklingMatrixMultiplication-0.1-SNAPSHOT.jar \
    hdfs://${MASTER_PRIVATE_IP}:9000/pratik/matrix_multiplication hdfs://${MASTER_PRIVATE_IP}:9000/spark/applicationHistory ${NUM_OF_COLS_IN_LEFT} > execution_info.log &
    ```

**NOTE**

1. You will need to setup spark history server.
2. Python indententation and tabs = 4 spaces.
3. Bash script indententation and tabs = 2 spaces.