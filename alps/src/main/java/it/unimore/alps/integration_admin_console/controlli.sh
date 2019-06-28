#!/bin/bash

#input parameters
project_jar=$1;
data_path=$2;
log_dir=$3;
path_csv_export=$4;
db=$5;
dedup_db=$6;

#check that project_jar is an absolute path to an executable jar file
case $project_jar in
	/*.jar)	if test ! -f $project_jar
	    then
	    	echo "$project_jar not an executable file"
	    	exit 2;
	    fi;;
	*)	echo "$project_jar not absolute path"
		exit 2;;
esac

#check that data_path is an absolute path to an accessible directory
case $data_path in
	/*)	if test ! -d $data_path -o ! -x $data_path
		then
			echo "$data_path not accessible directory"
			exit 2;
		fi;;
	*)	echo "$data_path not absolute path"
		exit 2;;
esac

#check that db does not contain /
case $db in
	*/*) echo "$db contains / character"
		 exit 2;;
esac