Setup: 
Need to comment bellow variables and restart the ubantu
unset PYSPARK_DRIVER_PYTHON
unset PYSPARK_DRIVER_PYTHON_OPTS

start hadoop and yarn services make sure hdfs has this /user/talentum structure
start hive metastore and hive server

download the zip
unzip the zip

do the changes in the property file
I have added onlt handful of indian cusines It can be change according to the requirement

run setup_local_dir.sh to setup the local directory structure
copy the json file from source to zomato_raw_files

go to the location zomato_etl/script

you can run wrapper.sh in many ways

wrapper.sh => which will run all the modules
wrapper.sh 1 => which will run first module
wrapper.sh 2 => which will run second module
wrapper.sh 3 => which will run third module

wrapper.sh 3 YYYYMMDD_HHMMSS country name to run the code manualy for only module 3
or 
wrapper.sh M YYYYMMDD_HHMMSS country name to run the code manualy for all modules
