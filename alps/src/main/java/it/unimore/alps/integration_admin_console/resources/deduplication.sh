#!/bin/bash
# invocation example
# bash data_import.sh alpsv13 /home/matteop/log/ /home/matteop/script/csv_data_23_10_2018/

# input parameters
db=$1;
log_dir=$2;
path_csv_export=$3;

# CHECK PARAMETERS

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

# BEGIN deduplication -----------------------------------------------------------------------------
echo "Starting deduplication..."
deduplicator1="java -Xmx110g -cp /home/paolos/winter-1.2-jar-with-dependencies.jar it.unimore.deduplication.ProjectDeduplication ${path_csv_export} &> ${log_dir}logProjectDedup_${db}.log";
deduplicator2="java -Xmx110g -cp /home/paolos/winter-1.2-jar-with-dependencies.jar it.unimore.deduplication.OrganizationDeduplication ${path_csv_export} labelQgrams aaa &> ${log_dir}logOrganizationDedup_LabelQGrams_${db}.log";
deduplicator3="java -Xmx100g -cp /home/paolos/alps-0.0.1-SNAPSHOT.jar it.unimore.alps.deduplication.DeduplicatorUtilTest -correpondenceFileOrgs correspondenceOrganizations_labelQgrams.tsv -correpondenceFilePrjs correspondenceProjects.tsv -correpondenceManualFileOrgs /home/paolos/manualCorrespondence.tsv -sourceDB ${db} -destinationDB ${dedup_db} &> ${log_dir}logDedupInsertion_Orcid_${db}.log";
deduplicators=("$deduplicator1" "$deduplicator2" "$deduplicator3");
#deduplicators=("$deduplicator3");
for i in "${deduplicators[@]}"; do
	#"${deduplicators[i]}";
	echo "${i[@]}";
#Per il momento commentato
#        eval "${i[@]}";
	retval=$?;
	if [ $retval -ne 0 ]; then
	    echo "Error in deduplication phase.";
	    exit 1;
	fi	
done
echo "Deduplication successfully completed."
# END deduplication -------------------------------------------------------------------------------
