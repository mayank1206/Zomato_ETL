#!/bin/bash

# Load properties file
. ./basic_details.properties

#remove the file from hdfs if exits
hadoop fs -rm -r -f $hdfs_root_dir/"zomato_etl_${user_name}"

# Create directory structure on HDFS
hadoop fs -mkdir -p $hdfs_root_dir/"zomato_etl_${user_name}/log"
hadoop fs -mkdir -p $hdfs_root_dir/"zomato_etl_${user_name}/zomato_ext/zomato"
hadoop fs -mkdir -p $hdfs_root_dir/"zomato_etl_${user_name}/zomato_ext/dim_country"

echo "Directory structure created successfully on HDFS"
