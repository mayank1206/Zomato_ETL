#!/bin/bash

#Load properties file
. ./basic_details.properties

if [ -z "$(ls -A "$project_dir"/zomato_etl/temp)" ]
then
    touch $project_dir/"zomato_etl/temp/_INPROGRESS"
else
    echo "Other process is running"
    exit 1
fi

echo "Module 2 Started"

#creating log table if not exists
beeline -u jdbc:hive2://localhost:10000/$db_name -n $hive_user_name -p $hive_password --hivevar dbName=$db_name --hivevar tableLocation=$hdfs_root_dir/"zomato_etl_${user_name}" -f $project_dir/"zomato_etl/hive/ddl/create_log_table.hive"
beeline -u jdbc:hive2://localhost:10000/$db_name -n $hive_user_name -p $hive_password --hivevar dbName=$db_name --hivevar tableLocation=$hdfs_root_dir/"zomato_etl_${user_name}" -f $project_dir/"zomato_etl/hive/ddl/create_dim_country.hive"

country_code_file_path=$project_dir/"zomato_etl/source/csv/country_code.csv"
beeline -u jdbc:hive2://localhost:10000/$db_name -n $hive_user_name -p $hive_password --hivevar dbName=$db_name --hivevar filePath=$country_code_file_path -f $project_dir/"zomato_etl/hive/dml/load_dim_country.hive"

beeline -u jdbc:hive2://localhost:10000/$db_name -n $hive_user_name -p $hive_password --hivevar dbName=$db_name --hivevar tableLocation=$hdfs_root_dir/"zomato_etl_${user_name}" -f $project_dir/"zomato_etl/hive/ddl/create_zomato.hive"	

csv_path=$project_dir/"zomato_etl/source/csv"

for file in $csv_path/zomato_*.csv
do		
	log_file=$project_dir/"zomato_etl/logs/log_$(date +%d%m%Y_%H%M%S).log"
	job_id=$(date +%d%m%Y%H%M%S)
	job_step="module_2"
	spark_submit_command="NA"
	start_time=$(date +"%Y-%m-%d %H:%M:%S")
	file_date=$(echo "$file" | rev | cut -c 5-19 | rev)
	beeline -u jdbc:hive2://localhost:10000/$db_name -n $hive_user_name -p $hive_password --hivevar dbName=$db_name --hivevar fileDate="${file_date}" --hivevar filePath=$file -f $project_dir/"zomato_etl/hive/dml/load_data_zomato.hive"

	if [ $? -eq 0 ]
	then
		end_time=$(date +"%Y-%m-%d %H:%M:%S")
		echo $job_id","$job_step","$spark_submit_command","$start_time","$end_time",SUCCEEDED" >> $log_file
		beeline -u jdbc:hive2://localhost:10000/$db_name -n $hive_user_name -p $hive_password --hivevar dbName=$db_name --hivevar filePath=$log_file -f $project_dir/"zomato_etl/hive/dml/load_log_table.hive"

	else
		end_time=$(date +"%Y-%m-%d %H:%M:%S")
		echo $job_id","$job_step","$spark_submit_command","$start_time","$end_time",FAILED" >> $log_file
		beeline -u jdbc:hive2://localhost:10000/$db_name -n $hive_user_name -p $hive_password --hivevar dbName=$db_name --hivevar filePath=$log_file -f $project_dir/"zomato_etl/hive/dml/load_log_table.hive"

		rm $project_dir/"zomato_etl/temp/_INPROGRESS"
		#echo "Module 2 failed" | mail -s "Module 2" $pm_mail
		echo "Job Failed"
		exit 1
	fi

	
done

rm $project_dir/"zomato_etl/temp/_INPROGRESS"

echo "Module 2 Completed"
