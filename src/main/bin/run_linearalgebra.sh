#!/bin/bash
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


if [ "$1" == "-h" ]; then
    echo "Usage: $(basename "${0}") {BASE_PATH} {TYPE} {BLOCK_ROW} {BLOCK_COL}"
    exit 0
fi

if [ $# -lt 4 ]
then
    echo "Missing Operand"
    echo "Run $(basename "${0}") -h for usage"
    exit 0
fi

echo "Your Input :-"
echo "BASE_PATH - ${1}"
echo "TYPE - ${2}"
echo "Block Row - ${3}"
echo "Block Column - ${4}"


spark-submit \
--class edu.asu.linearalgebra.Main \
--master spark://"${SPARK_MASTER}" \
--conf spark.default.parallelism="${SPARK_DEFAULT_PAR}" \
--deploy-mode client \
"${APP_HOME}"/lib/Spark-Partitioning-0.1-SNAPSHOT.jar \
hdfs://"${HADOOP_MASTER}${1}" "${2}" "${3}" "${4}"
