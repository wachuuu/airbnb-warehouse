#!/bin/bash
INPUT_DIR="gs://pbd22gl2gw/project2"

function run_etl() {
  spark-submit --class $1 \
  --master yarn --num-executors 5 --driver-memory 6g \
  --executor-memory 6g --executor-cores 4 $2 $INPUT_DIR
}

function create_tables() {
  spark-submit --class com.bigdata.spark.SetUp \
  --master yarn --num-executors 5 --driver-memory 512m \
  --executor-memory 512m --executor-cores 1 tables-creation.jar
}

# create tables
echo "------------------------------------"
echo "Creating tables..."
create_tables
echo "Tables created sucessfully."

# room_type ETL
echo "------------------------------------"
echo "Runnig ETL script for dimension 'room_type'"
run_etl com.bigdata.spark.ETL room_type-etl.jar

# neighbourhood ETL
echo "------------------------------------"
echo "Runnig ETL script for dimension 'neighbourhood'"
run_etl com.bigdata.spark.ETL neighbourhood-etl.jar

# date ETL
echo "------------------------------------"
echo "Runnig ETL script for dimension 'date'"
run_etl com.bigdata.spark.ETL date-etl.jar

# host ETL
echo "------------------------------------"
echo "Runnig ETL script for dimension 'host'"
run_etl com.bigdata.spark.ETL host-etl.jar

# table of facts ETL
echo "------------------------------------"
echo "Runnig ETL script for table of facts"
run_etl com.bigdata.facts.ETL facts-etl.jar

echo "------------------------------------"
echo "Creating data warehouse ended."
echo "------------------------------------"
