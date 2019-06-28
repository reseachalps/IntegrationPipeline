#!/bin/bash
# invocation example
# bash data_import.sh /home/matteop/script/alps-0.0.1-SNAPSHOT.jar alpsv13 /home/matteop/log/ /home/matteop/script/csv_data_23_10_2018/

# input parameters
project_jar=$1;
db=$2;
log_dir=$3;
path_csv_export=$4;

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

#check that path_csv_export is an absolute path to an accessible directory
case $path_csv_export in
	/*)	if test ! -d $path_csv_export -o ! -x $path_csv_export
		then
			echo "$path_csv_export not accessible directory"
			exit 2;
		fi;;
	*)	echo "$path_csv_export not absolute path"
		exit 2;;
esac

#check that db does not contain /
case $db in
	*/*) echo "$db contains / character"
		 exit 2;;
esac

# BEGIN CSV export --------------------------------------------------------------------------------
echo "Starting exporting data in CSV..."
#Per ora lo dichiaramo e basta, ricorda di eliminare anche le virgolette
echo "java -Xmx20g -cp ${project_jar} it.unimore.alps.exporter.NewExporterCSV -sourceName "All" -DB ${db} -outputFolder ${path_csv_export} &> ${log_dir}log_exporterCSV_${db}.log";
retval=$?;
if [ $retval -ne 0 ]; then
    echo "Error in exporting csv file.";
    exit 1;
fi
echo "CSV export successfully completed."
# END csv export ----------------------------------------------------------------------------------
