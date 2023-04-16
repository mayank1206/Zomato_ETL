#!/bin/bash

#Load properties file
. ./basic_details.properties

#Setting up the Hdfs directories
source $project_dir/"zomato_etl/script/set_hdfs.sh"

#Recreate the Hive table
beeline -u jdbc:hive2://localhost:10000/$db_name -n $hive_user_name -p $hive_password --hivevar dbName=$db_name -f $project_dir/"zomato_etl/hive/ddl/clean_hive.hive"

#checking file present json location
json_dir=$project_dir/"zomato_etl/source/json/"
staging_dir=$project_dir/"zomato_raw_files/"

if [ ! -e "${json_dir}*" ]
then
    ls $staging_dir | head -3 | xargs -I {} cp $staging_dir{} $json_dir 
    echo "Files are copied from staging to json location"
else
    echo "File already exist in json location"
fi

# Remove archive and temp file
rm -f $project_dir/"zomato_etl/archive"/*


if [ -d $project_dir/"zomato_etl/temp" ]
then
    rm -rf $project_dir/"zomato_etl/temp/*"
else
    mkdir $project_dir/"zomato_etl/temp"
fi
