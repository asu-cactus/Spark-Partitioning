### Spark-Partitioning

Investigating and benchmarking how partitioning of data on HDFS will affect Spark performance

#### Steps to execute the experiments.

1. Build using the command - `mvn clean package`. It will create a tar.gz file named `Spark-Partitioning-0.1-SNAPSHOT.tar.gz` with the 4 directories `bin`, `etc`, `python` and `lib`.
Copy the `tar.gz` file to the cluster and decompress the folder.

2. To create random matrices and load them to HDFS, execute the shell script `nohup ./bin/load_data.sh ${ROW_LEFT} {COL_LEFT} ${ROW_RIGHT} ${COL_RIGHT} ${WORK_FLOW = (SPARK, SQL)} ${BASE_PATH} > load_data.logs &`.

3. To execute a particular experiment, execute the shell script `nohup ./bin/run_experiment.sh ${WORK_FLOW = (SPARK, SQL, BUCKET)} ${BASE_PATH} ${EXPERIMENT} ${NUM_OF_PARTITIONS} > job_${EXPERIMENT}_${PARTITIONS}.logs &`.
Allowed values for `${EXPERIMENT}` are `e1`, `e2` or `e3`. NOTE: With `BUCKET` type workload `e3` experiment is not valid.

4. To execute naive implementation of `PageRank` (normal code and with co-partitioning both) on Spark, execute command
  `nohup ./bin/run_page_rank.sh ${NUM_OF_PAGES} ${MAX_LINKS} ${RAW_DATA_OP_NM} ${BASE_PATH} > rank.logs &`.

**Code style notes**
1. Python indentation and tabs = 4 spaces. (We are using Python 3)
2. Bash script indentation and tabs = 2 spaces.
3. Set up the Scalafmt plugin and use the `.scalafmt.conf` for auto formatting.

**Git strategy**
1. Always fetch/pull changes from the central repository before committing any changes.
2. When you are pulling changes from shared/public branches (e.g. `master` and `test`),
use merge strategy. Example command sequence would be:
    1. `git checkout ${YOUR_BRANCH}`
    2. `git merge test`
3. When you want to push your changes to the shared/public branches, create a pull request on the shared/public branches (e.g. `test`).
Use rebase and merge to accept the pull requests.