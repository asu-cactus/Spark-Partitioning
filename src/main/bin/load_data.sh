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


# You can use the $APP_HOME as the absolute path to your project dir
# This makes navigating your project files easier.
# This script is run after running scp -i {YOUR_KEY} {PATH/TO/JAR_FILE} {USER}@18.191.83.127
# Follow the next steps for running an experiment on the cluster
# 1) Run Python3 {APP_HOME}/python/random_generator.py {r1} {c1} {r2} {c2} to create the left_matrix.txt and right_matrix.txt
# 2) Remove existing files in the HDFS using hdfs dfs -rm {PATH/TO/FILE}
# 3) Place the txt files in the HDFS using -
#     hdfs dfs -put $APP_HOME/bin/left_matrix.txt $BASE_PATH/raw/left/ and 
#     hdfs dfs -put $APP_HOME/bin/right_matrix.txt $BASE_PATH/raw/right/
# 4) Delete the left and right matrix txt files from the disk - 
#     rm $APP_HOME/bin/left_matrix.txt
#     rm $APP_HOME/bin/right_matrix.txt
# 5) Run the command - 
#   nohup spark-submit \
#   --class edu.asu.sparkpartitioning.TextToObjectFiles \
#   --master spark://172.31.19.91:7077 \
#   --deploy-mode client \
#   ${PATH_TO_JAR}/Spark-Partitioning-0.1-SNAPSHOT.jar \
#   hdfs://172.31.19.91:9000/{BASE_PATH} hdfs://172.31.19.91:9000/{HISTORY_LOG_DIR} > parsing_logs.log &



#   This will convert the txt files to objectfiles
###############################################################################################################
# 6)Run the command - 
#   nohup spark-submit \
#   --class edu.asu.sparkpartitioning.Main \
#   --master spark://172.31.19.91:7077 \
#   --deploy-mode client \
#   ${PATH_TO_JAR}/Spark-Partitioning-0.1-SNAPSHOT.jar \
#   hdfs://172.31.19.91:9000/{BASE_PATH} hdfs://172.31.19.91:9000/{HISTORY_LOG_DIR} ${NUM_PARTITION} ${EXPERIMENT} > job_logs_${NUM_PARTITION}.log &

#   This will execute the experiment based on ${EXPERIMENT}
# 7) Delete everything in the {BASE_PATH}/experiment in the hdfs using 
#     hdfs dfs -rm -r {BASE_PATH}/{EXPERIMENT}/*

if [ "$1" == "-h" ]; then
  echo "Usage: `basename $0` {r1} {c1} {r2} {c2} {BASE_PATH}"
  exit 0
fi

if [ $# -lt 5 ]
then
  echo "Missing Operand"
  echo "Run `basename $0` -h for usage"
  exit 0
fi

printf "Your Input - \n"
printf "\tr1 - $1\n"
printf "\tc1 - $2\n"
printf "\tr2 - $3\n"
printf "\tc2 - $4\n" 
printf "\tBase Path - $5\n"

if [ $2 != $3 ]
then 
  echo "Error: c1 and r2 should be equal for matrix multiplication"
  echo "Run `basename $0` -h for usage"
  exit 0
fi

BASE_PATH=$5
PWD=$(pwd)

hdfs dfs -rm -r $BASE_PATH/raw/left/*
hdfs dfs -rm -r $BASE_PATH/raw/right/*
hdfs dfs -rm -r $BASE_PATH/common/*

# creating the matrices with the python file
python3 $APP_HOME/python/random_generator.py $1 $2 $3 $4

# loading the left and right matrices into the HDFS
hdfs dfs -put -f $PWD/left_matrix.txt $BASE_PATH/raw/left/
hdfs dfs -put -f $PWD/right_matrix.txt $BASE_PATH/raw/right/

# delete the left and right matrices from the disk
rm $PWD/left_matrix.txt
rm $PWD/right_matrix.txt

# running the spark command to convert txt files to objectfiles
spark-submit \
  --class edu.asu.sparkpartitioning.TextToObjectFiles \
  --master spark://172.31.19.91:7077 \
  --deploy-mode client \
  $APP_HOME/lib/Spark-Partitioning-0.1-SNAPSHOT.jar \
  hdfs://172.31.19.91:9000$BASE_PATH hdfs://172.31.19.91:9000/spark/applicationHistory > parsing_logs.log 


