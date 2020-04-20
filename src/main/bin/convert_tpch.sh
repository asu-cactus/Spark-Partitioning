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
if [ "${1}" == "-h" ];
then
  echo "Usage: $(basename "${0}") {BASE_PATH}"
  exit 0
fi

# checking if the number of args to the script are proper
if [ $# -lt 1 ]
then
  echo "Missing Operand"
  echo "Run $(basename "${0}") -h for usage"
  exit 0
fi

echo "Your Input :- "
echo "BASE_PATH - ${1}"

PWD="$(pwd)"

# Clear if previous execution data
hdfs dfs -rm -r -skipTrash "${1}"/parquet
hdfs dfs -rm -r -skipTrash "${1}"/parquet_parts
hdfs dfs -rm -r -skipTrash "${1}"/parquet_buckets

# Spark application to read raw TPC-H data
# and convert it to Parquet format.
spark-submit \
--class edu.asu.tpch.RawToParquet \
--master spark://172.31.19.91:7077 \
--conf spark.default.parallelism="80" \
--deploy-mode client \
"${APP_HOME}"/lib/Spark-Partitioning-0.1-SNAPSHOT.jar \
hdfs://172.31.19.91:9000"${1}" \
hdfs://172.31.19.91:9000/spark/applicationHistory