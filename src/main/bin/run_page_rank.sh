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
  echo "Usage: $(basename "${0}") {NUM_OF_PAGES} {MAX_LINKS}\
  {RAW_DATA_OP_NM} {BASE_PATH}"
  exit 0
fi

# checking if the number of args to the script are proper
if [ $# -lt 4 ]
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

PWD="$(pwd)"

# Clear if previous execution data
hdfs dfs -rm -r "${BASE_PATH}"/page_rank
# Create random data for page rank algorithm
python3 "${APP_HOME}"/python/pagerank_generator.py "${1}" "${2}" "${3}"
# Create the Page Rank raw dir
hdfs dfs -mkdir -p "${BASE_PATH}"/page_rank/raw/
# Load the raw text file of page rank data on HDFS
hdfs dfs -put -f "${PWD}"/"${3}" "${BASE_PATH}"/page_rank/raw/
# Delete the raw file from the local file system
rm "${PWD}"/"${3}"

# Run Spark code for the Page Rank algorithm WITHOUT co-partitioning
spark-submit \
--class edu.asu.pagerank.Main \
--master spark://172.31.19.91:7077 \
--deploy-mode client \
"${APP_HOME}"/lib/Spark-Partitioning-0.1-SNAPSHOT.jar \
hdfs://172.31.19.91:9000"${BASE_PATH}" \
hdfs://172.31.19.91:9000/spark/applicationHistory \
"NO_partition"

# Run Spark code for the Page Rank algorithm WITH COMMON partitioners
spark-submit \
--class edu.asu.pagerank.Main \
--master spark://172.31.19.91:7077 \
--deploy-mode client \
"${APP_HOME}"/lib/Spark-Partitioning-0.1-SNAPSHOT.jar \
hdfs://172.31.19.91:9000"${BASE_PATH}" \
hdfs://172.31.19.91:9000/spark/applicationHistory \
"CO_partitioned"

# If you need to clear the page rank directory from HDFS
# after the execution is completed, comment the command below
# hdfs dfs -rm -r "${BASE_PATH}"/page_rank




