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
  echo "Usage: $(basename "${0}") {Total number of tree} \
  {Average Number of nodes} {Lower Bound} {Number of Threads} \
  {Batch Size}"
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
echo "Total number of trees - ${1}"
echo "Average Number of nodes - ${2}"
echo "Lower Bound - ${3}"
echo "Number of Threads - ${4}"
echo "Batch Size - ${5}"

java -cp "${APP_HOME}"/lib/Spark-Partitioning-0.1-SNAPSHOT.jar \
edu.asu.overheadanalysis.graphmatching.Main \
"${1}" "${2}" "${3}" "${4}" "${5}"
