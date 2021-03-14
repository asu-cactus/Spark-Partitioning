#!/bin/bash

# Find the script file home
find_script_home() {
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
}

input_checks() {
  # help for usage of the script
  if [ "$1" == "-h" ]; 
  then
    echo "Usage: $(basename "${0}") {r1} {c1} {r2} {c2} {WORK_FLOW = (RDD, SPARK)} {BASE_PATH} {NUM_OF_PARTS} {DENSITY}"
    exit 0
  fi

  # checking if the number of args to the script are proper
  if [ $# -lt 8 ]
  then
    echo "Missing Operand"
    echo "Run $(basename "${0}") -h for usage"
    exit 0
  fi

  echo "Your Input :- "
  echo "r1 - ${1}"
  echo "c1 - ${2}"
  echo "r2 - ${3}"
  echo "c2 - ${4}"
  echo "Work Flow - ${5}"
  echo "Base Path - ${6}"
  echo "Num of Parts - ${7}"
  echo "DENSITY - ${8}"
  
  # checking if c1 == r2 which is required for matrix multiplication
  if [ "${2}" != "${3}" ]
  then 
    echo "Error: c1 and r2 should be equal for matrix multiplication"
    echo "Run $(basename "${0}") -h for usage"
    exit 0
  fi
}

check_HDFS_directories() {
  
  BASE_PATH=$1
  
  # check if raw folder exists
  if [[ $(hdfs dfs -test -d "${BASE_PATH}"/raw) -eq 0 ]]
  then
    echo "HDFS directores not found. Creating raw, raw/left and raw/right"
    hdfs dfs -mkdir -p "${BASE_PATH}"/raw
    hdfs dfs -mkdir -p "${BASE_PATH}"/raw/left
    hdfs dfs -mkdir -p "${BASE_PATH}"/raw/right
  else
    #check if raw is empty
    isRawEmpty=$(hdfs dfs -count "${BASE_PATH}"/raw | awk '{print $2}')
    if [[ $isRawEmpty -eq 0 ]]
    then
      #create raw/left and raw/right
      hdfs dfs -mkdir -p "${BASE_PATH}"/raw/left
      hdfs dfs -mkdir -p "${BASE_PATH}"/raw/right
    else
      #check if raw/left and raw/right are empty or not
      isEmptyLeft=$(hdfs dfs -count "${BASE_PATH}"/raw/left | awk '{print $2}')
      isEmptyRight=$(hdfs dfs -count "${BASE_PATH}"/raw/right | awk '{print $2}')
      if [[ $isEmptyLeft -eq 0 ]] && [[ $isEmptyRight -eq 0 ]]
      then
        true
      else
        hdfs dfs -rm -r -skipTrash "${BASE_PATH}"/raw/left/*
        hdfs dfs -rm -r -skipTrash "${BASE_PATH}"/raw/right/*
      fi
    fi
  fi

   # check if common folder is present in the HDFS
   if [[ $(hdfs dfs -test -d $BASE_PATH/common) -eq 0 ]]
   then
     echo "HDFS directores not found. Creating common directory"
     hdfs dfs -mkdir -p "${BASE_PATH}"/common
   else
     hdfs dfs -rm -r $BASE_PATH/common/*
   fi

}

main() {
  find_script_home
  input_checks "${@}"
  WORK_FLOW=$5
  BASE_PATH=$6
  PWD=$(pwd)

  #check if all the directories required are present in the HDFS
  check_HDFS_directories "${BASE_PATH}"

  # creating the matrices with the python file
  python3 "${APP_HOME}"/python/random_generator.py \
  "${1}" "${2}" "${3}" "${4}" "${8}"

  # loading the left and right matrices into the HDFS
  hdfs dfs -put -f "${PWD}"/left_matrix.txt "${BASE_PATH}"/raw/left/
  hdfs dfs -put -f "${PWD}"/right_matrix.txt "${BASE_PATH}"/raw/right/

  # delete the left and right matrices from the disk
  rm "${PWD}"/left_matrix.txt
  rm "${PWD}"/right_matrix.txt

  # running the workflow according to input to convert to either objectfiles or parquet files
  case $WORK_FLOW in

  "RDD")
    echo "RDD"
    spark-submit \
    --class edu.asu.sparkpartitioning.TextToObjectFiles \
    --conf spark.default.parallelism="${SPARK_DEFAULT_PAR}" \
    --master spark://"${SPARK_MASTER}" \
    --deploy-mode client \
    "${APP_HOME}"/lib/Spark-Partitioning-0.1-SNAPSHOT.jar \
    hdfs://"${HADOOP_MASTER}${BASE_PATH}"
    ;;

  "SQL")
    echo "SQL"

    spark-submit \
    --class edu.asu.sqlpartitioning.TextToParquetFiles \
    --conf spark.default.parallelism="${SPARK_DEFAULT_PAR}" \
    --master spark://"${SPARK_MASTER}" \
    --deploy-mode client \
    "${APP_HOME}"/lib/Spark-Partitioning-0.1-SNAPSHOT.jar \
    hdfs://"${HADOOP_MASTER}${BASE_PATH}" "${7}"
    ;;
  esac

  # Forcing the replication to be 1
  hdfs dfs -setrep -w 1 "${BASE_PATH}"

  hdfs dfs -rm -r -skipTrash "${BASE_PATH}"/raw/left
  hdfs dfs -rm -r -skipTrash "${BASE_PATH}"/raw/right
}

main "${@}"

