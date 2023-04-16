#!/bin/bash

# Load properties file
. ./basic_details.properties

# backup and removal of folder
if [ -d $project_dir/"zomato_etl" ]
then
    #zip -r "zomato_backup_$(date +%s).zip" $project_dir/"zomato_etl"
    #echo "Backup is take"
    rm -r $project_dir/"zomato_etl"
    echo "Removed the zomato_etl folder"
fi

# create directories
mkdir -p $project_dir/"zomato_etl/source/json"
mkdir -p $project_dir/"zomato_etl/source/csv"
mkdir -p $project_dir/"zomato_etl/archive"
mkdir -p $project_dir/"zomato_etl/hive/ddl"
mkdir -p $project_dir/"zomato_etl/hive/dml"
mkdir -p $project_dir/"zomato_etl/spark/jars"
mkdir -p $project_dir/"zomato_etl/spark/py"
mkdir -p $project_dir/"zomato_etl/script"
mkdir -p $project_dir/"zomato_etl/logs"
mkdir -p $project_dir/"zomato_raw_files"

echo "Directory structure created"

#copy the code file to the respected location

cp ./*.py $project_dir/"zomato_etl/spark/py"
cp ./*.sh $project_dir/"zomato_etl/script"
cp ./*.properties $project_dir/"zomato_etl/script"
cp ./create_*.hive $project_dir/"zomato_etl/hive/ddl"
cp ./clean_*.hive $project_dir/"zomato_etl/hive/ddl"
cp ./load_*.hive $project_dir/"zomato_etl/hive/dml"
cp ./country_code.csv  $project_dir/"zomato_etl/source/csv"
echo "Files are coppied successfully"
