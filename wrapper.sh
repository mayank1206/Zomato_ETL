#!/bin/bash

#Load properties file
. ./basic_details.properties

# invoke the setup script

source $project_dir/"zomato_etl/script/setup.sh"

case $mode in
1)
	source $project_dir/"zomato_etl/script/module_1.sh"
;;
2)
	source $project_dir/"zomato_etl/script/module_2.sh"
;;
3)	
	source $project_dir/"zomato_etl/script/module_3.sh" $2 $3
;;
*)	
	source $project_dir/"zomato_etl/script/module_1.sh"
	if [ $? -eq 0 ]
	then
    		source $project_dir/"zomato_etl/script/module_2.sh"
                if [ $? -eq 0 ]
		then
	    		source $project_dir/"zomato_etl/script/module_3.sh" $2 $3
	        fi
	fi
	
	
;;
esac   





