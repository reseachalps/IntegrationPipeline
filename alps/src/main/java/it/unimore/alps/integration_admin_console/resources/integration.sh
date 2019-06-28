#!/bin/bash
# invocation example
# bash data_import.sh /home/matteop/script/alps-0.0.1-SNAPSHOT.jar alpsv13 /home/matteop/log/

# input parameters
project_jar=$1;
db=$2;
log_dir=$3;

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

# BEGIN data integration --------------------------------------------------------------------------
echo "Starting integration..."
integrator1="java -Xmx10g -cp ${project_jar} it.unimore.alps.integrator.WebsiteIntegrator -DB $db -WebsiteDB websitecorrect1 &> ${log_dir}log_website_corrector_${db}.log";
#integrator3="java -Xmx10g -cp ${project_jar} it.unimore.alps.integrator.LatLongFinale -DB $db -LatLonDB geocoordinate &> ${log_dir}log_latlon_integrator_${db}.log";
integrator2="java -Xmx30g -cp ${project_jar} it.unimore.alps.integrator.GeoCoordinatesIntegrator -DB $db -LatLonDB geocoordinate &> ${log_dir}log_latlon_integrator_${db}.log";
integrators=("$integrator1" "$integrator2");
for j in "${integrators[@]}"; do
	#"${integrators[j]}";
	echo "${j[@]}";
#per ora non lo eseguiamo
#        eval "${j[@]}";
	retval=$?;
	if [ $retval -ne 0 ]; then
	    echo "Error in integration phase.";
	    exit 1;
	fi	
done
echo "Data integration successfully completed."
# END data integration ----------------------------------------------------------------------------