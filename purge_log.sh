#!/bin/bash

#Load properties file
. ./basic_details.properties

find $project_dir/"zomato_etl/logs" -type f -mtime +7 -exec rm {} \;
