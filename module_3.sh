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

echo "Module 3 Started"

#creating log table if not exists
beeline -u jdbc:hive2://localhost:10000/$db_name -n $hive_user_name -p $hive_password --hivevar dbName=$db_name --hivevar tableLocation=$hdfs_root_dir/"zomato_etl_${user_name}" -f $project_dir/"zomato_etl/hive/ddl/create_log_table.hive"

beeline -u jdbc:hive2://localhost:10000/$db_name -n $hive_user_name -p $hive_password --hivevar dbName=$db_name -f $project_dir/"zomato_etl/hive/ddl/create_zomato_summary.hive"	

python_path=$project_dir/"zomato_etl/spark/py/load_data_zomato_summary.py"
echo "Number of arguments: $#"
if [ $# -lt 2 ]
then
    file_date="None"
    country="None"
else
    # check for blank value probably needed
    file_date=$1
    country=$2
fi


log_file=$project_dir/"zomato_etl/logs/log_$(date +%d%m%Y_%H%M%S).log"
job_id=$(date +%d%m%Y%H%M%S)
job_step="module_3"
Id=mod_3_$(date +"%Y%m%d%H%M%S")
spark_submit_command="spark-submit --master yarn --deploy-mode client --driver-memory 2g --num-executors 2 --executor-memory 1g ${python_path} ${Id} ${db_name} ${user_id} ${create_datetime} ${file_date} ${country} ${indian_cusines}"
start_time=$(date +"%Y-%m-%d %H:%M:%S")

		
spark-submit --master yarn --deploy-mode client --driver-memory 2g --num-executors 2 --executor-memory 1g $python_path $Id $db_name $user_id $create_datetime "$indian_cusines" $file_date $country 
appId=$(yarn application -list -appStates ALL | grep $Id | awk '{print $1}')
appState=$(yarn application -status $appId | grep -oP '(?<=Final-State : ).*')

if [[ "$appState" == "SUCCEEDED" ]]
then
	end_time=$(date +"%Y-%m-%d %H:%M:%S")
	echo $job_id","$job_step","$spark_submit_command","$start_time","$end_time",SUCCEEDED" >> $log_file
	beeline -u jdbc:hive2://localhost:10000/$db_name -n $hive_user_name -p $hive_password --hivevar dbName=$db_name --hivevar filePath=$log_file -f $project_dir/"zomato_etl/hive/dml/load_log_table.hive"

else
	end_time=$(date +"%Y-%m-%d %H:%M:%S")
	echo $job_id","$job_step","$spark_submit_command","$start_time","$end_time",FAILED" >> $log_file
	beeline -u jdbc:hive2://localhost:10000/$db_name -n $hive_user_name -p $hive_password --hivevar dbName=$db_name --hivevar filePath=$log_file -f $project_dir/"zomato_etl/hive/dml/load_log_table.hive"

	rm $project_dir/"zomato_etl/temp/_INPROGRESS"
	#echo "Module 3 failed" | mail -s "Module 3" $pm_mail
	echo "Module 3 Failed"
	exit 1
fi

rm $project_dir/"zomato_etl/temp/_INPROGRESS"
echo "Module 3 Completed"
