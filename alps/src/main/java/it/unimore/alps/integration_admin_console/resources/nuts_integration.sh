#!/bin/bash
# invocation example
# bash data_import.sh /home/matteop/script/alps-0.0.1-SNAPSHOT.jar alpsv13 /home/matteop/log/ /home/matteop/script/csv_data_23_10_2018/

# input parameters
project_jar=$1;
db=$2;
log_dir=$3;
dedup_db=$4;

# CHECK PARAMETERS
#check that project_jar is an absolute path to an readable jar file
case $project_jar in
	/*.jar)	if test ! -f $project_jar -o ! -r $project_jar
	    then
	    	echo "$project_jar not a readable jar file"
	    	exit 2;
	    fi;;
	*)	echo "$project_jar not absolute path"
		exit 2;;
esac

#check that log_dir is an absolute path to an accessible directory
case $log_dir in
	/*)	if test ! -d $log_dir -o ! -x $log_dir
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

#check that db does not contain /
case $dedup_db in
	*/*) echo "dedup_db contains / character"
		 exit 2;;
esac

# BEGIN NUTS INTEGRATOR ---------------------------------------------------------------------------
echo "Nuts codes integration";
#togliere echo e le virgolette, solo per testare
echo "java -cp ${project_jar} it.unimore.alps.integrator.NutsIntegrator -DB ${dedup_db} &> ${log_dir}log_nuts_integrator_${db}.log";
# END NUTS INTEGRATOR -----------------------------------------------------------------------------
