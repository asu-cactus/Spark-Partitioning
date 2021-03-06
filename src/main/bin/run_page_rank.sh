#!/bin/bash

# Find the script file home
pushd . > /dev/null
SCRIPT_DIRECTORY="${BASH_SOURCE[0]}";
while [ -h "${SCRIPT_DIRECTORY}" ];
do
  cd "$(dirname "${SCRIPT_DIRECTORY}")" || exit
  SCRIPT_DIRECTORY="$(readlink "$(basename "${SCRIPT_DIRECTORY}")")";
done
cd "$(dirname "${SCRIPT_DIRECTORY}")" > /dev/null || exit
SCRIPT_DIRECTORY="$(pwd)";
popd  > /dev/null || exit
APP_HOME="$(dirname "${SCRIPT_DIRECTORY}")"

# help for usage of the script
if [ "$1" == "-h" ];
then
  echo "Usage: $(basename "${0}") {NUM_OF_PAGES} {MAX_LINKS} \
  {RAW_DATA_OP_NM} {BASE_PATH} {NUM_OF_ITER}"
  exit 0
fi

# checking if the number of args to the script are proper
if [ $# -lt 5 ]
then
  echo "Missing Operand"
  echo "Run $(basename "${0}") -h for usage"
  exit 0
fi

echo "Your Input :- "
echo "NUM_OF_PAGES - ${1}"
echo "MAX_LINKS - ${2}"
echo "RAW_DATA_OP_NM - ${3}"
echo "BASE_PATH - ${4}"
echo "NUM_OF_ITER - ${5}"

PWD="$(pwd)"

# Clear if previous execution data
hdfs dfs -rm -r -skipTrash "${4}"/page_rank
# Create random data for page rank algorithm
python3 "${APP_HOME}"/python/pagerank_generator.py "${1}" "${2}" "${3}"
# Create the Page Rank raw dir
hdfs dfs -mkdir -p "${4}"/page_rank/raw/
# Load the raw text file of page rank data on HDFS
hdfs dfs -put -f "${PWD}"/"${3}" "${4}"/page_rank/raw/
# Delete the raw file from the local file system
rm "${PWD}"/"${3}"

# Run Spark code for the Page Rank algorithm WITHOUT co-partitioning
spark-submit \
--class edu.asu.pagerank.Main \
--master spark://"${SPARK_MASTER}" \
--conf spark.default.parallelism="${SPARK_DEFAULT_PAR}" \
--conf spark.rpc.askTimeout=360s \
--deploy-mode client \
"${APP_HOME}"/lib/Spark-Partitioning-0.1-SNAPSHOT.jar \
hdfs://"${HADOOP_MASTER}${4}" \
hdfs://"${HADOOP_MASTER}"/spark/applicationHistory \
"no_partition" "${5}"

# Sleep for 5 minutes to let all the previous processes close
sleep 5m

# Run Spark code for the Page Rank algorithm WITH COMMON partitioners
spark-submit \
--class edu.asu.pagerank.Main \
--master spark://"${SPARK_MASTER}" \
--conf spark.default.parallelism="${SPARK_DEFAULT_PAR}" \
--conf spark.rpc.askTimeout=360s \
--deploy-mode client \
"${APP_HOME}"/lib/Spark-Partitioning-0.1-SNAPSHOT.jar \
hdfs://"${HADOOP_MASTER}${4}" \
"with_partition" "${5}"

# If you need to clear the page rank directory from HDFS
# after the execution is completed, comment the command below
hdfs dfs -rm -r -skipTrash "${4}"/page_rank





