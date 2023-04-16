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

echo "Module 1 Started"

#creating log table if not exists
beeline -u jdbc:hive2://localhost:10000/$db_name -n $hive_user_name -p $hive_password --hivevar dbName=$db_name --hivevar tableLocation=$hdfs_root_dir/"zomato_etl_${user_name}" -f $project_dir/"zomato_etl/hive/ddl/create_log_table.hive"

csv_path=$project_dir/"zomato_etl/source/csv"
json_path=$project_dir/"zomato_etl/source/json"
python_path=$project_dir/"zomato_etl/spark/py/json_to_csv.py"

for file in $json_path/*.json
do
	source_path="file://${file}"
	folder_name="zomato_$(date +%Y%m%d_%H%M%S)"
        destination_path="file://${csv_path}/${folder_name}"	
	log_file=$project_dir/"zomato_etl/logs/log_$(date +%d%m%Y_%H%M%S).log"
	job_id=$(date +%d%m%Y%H%M%S)
	job_step="module_1"
	Id=mod_1_$(date +"%Y%m%d%H%M%S")
	spark_submit_command="spark-submit --master yarn --deploy-mode client --driver-memory 2g --num-executors 2 --executor-memory 1g ${python_path} ${source_path} ${destination_path} ${Id}"
	start_time=$(date +"%Y-%m-%d %H:%M:%S")	
	
	spark-submit --master yarn --deploy-mode client --driver-memory 2g --num-executors 2 --executor-memory 1g $python_path $source_path $destination_path $Id
	appId=$(yarn application -list -appStates ALL | grep $Id | awk '{print $1}')
	appState=$(yarn application -status $appId | grep -oP '(?<=Final-State : ).*')

	if [[ "$appState" == "SUCCEEDED" ]]
	then
		mv $csv_path/$folder_name/*.csv $csv_path/"${folder_name}.csv"
		rm -r $csv_path/$folder_name
		end_time=$(date +"%Y-%m-%d %H:%M:%S")
		echo $job_id","$job_step","$spark_submit_command","$start_time","$end_time",SUCCEEDED" >> $log_file
		beeline -u jdbc:hive2://localhost:10000/$db_name -n $hive_user_name -p $hive_password --hivevar dbName=$db_name --hivevar filePath=$log_file -f $project_dir/"zomato_etl/hive/dml/load_log_table.hive"

	else
		end_time=$(date +"%Y-%m-%d %H:%M:%S")
		echo $job_id","$job_step","$spark_submit_command","$start_time","$end_time",FAILED" >> $log_file
		beeline -u jdbc:hive2://localhost:10000/$db_name -n $hive_user_name -p $hive_password --hivevar dbName=$db_name --hivevar filePath=$log_file -f $project_dir/"zomato_etl/hive/dml/load_log_table.hive"

		rm $project_dir/"zomato_etl/temp/_INPROGRESS"
		#echo "Module 1 failed" | mail -s "Module 1" $pm_mail
		echo "Job Failed"
		exit 1
	fi
done

mv $json_path/* $project_dir/"zomato_etl/archive/"
rm $project_dir/"zomato_etl/temp/_INPROGRESS"

echo "Module 1 Completed"
