# Introduction:
ETL pipeline to extracting data from JSON files, modified it and loading it into a Hive data warehouse. The json file contains daily online order data from zomato site.

# Setup: 
1. Change the property file according to the need
2. Run setup_local_dir.sh to setup the local directory structure
3. Copy the json file from source to zomato_raw_files
4. Run the wrapper.sh

you can run wrapper.sh in below ways
wrapper.sh => which will run all the modules
wrapper.sh 1 => which will run first module
wrapper.sh 2 => which will run second module
wrapper.sh 3 => which will run third module

wrapper.sh 3 YYYYMMDD_HHMMSS country name to run the code manualy for only module 3
or 
wrapper.sh M YYYYMMDD_HHMMSS country name to run the code manualy for all modules
