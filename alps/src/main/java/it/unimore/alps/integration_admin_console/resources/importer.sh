#!/bin/bash
# invocation example
# bash data_import.sh /home/matteop/script/alps-0.0.1-SNAPSHOT.jar /home/matteop/new_data/ alpsv13 /home/matteop/log/ 12

# input parameters
project_jar=$1;
data_path=$2;
db=$3;
log_dir=$4;
N=$5;

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

#check that N is positive number less than 17
case $N in
	*[!0-9]*) echo "$N not a number"
		 exit 2;;
	*) if test $N -lt 0 -o $N -ge 17
	   then
		echo "$N out of bounds"
		 exit 2
	   fi;;
esac

# BEGIN data import -------------------------------------------------------------------------------
echo "Starting importing data...";
#ORCID
executor1="java -Xmx20g -cp ${project_jar} it.unimore.alps.sources.orcid.OrcidImporterNoDedup -DB $db -file ${data_path}orcid/research_alps_orcid.json &> ${log_dir}log_orcid_${db}.log";
#SCANR
executor2="java -Xmx20g -cp ${project_jar} it.unimore.alps.scanr.importer.ScanRImporter -DB $db -orgsFile ${data_path}scanr/entreprises.json -orgsRNSRFile ${data_path}scanr/rnsr.json -projectsFile ${data_path}scanr/projets.json -publicationsFile ${data_path}scanr/publications.json &> ${log_dir}log_scanr_${db}.log";
#ARIANNA
executor3="java -Xmx10g -cp ${project_jar} it.unimore.alps.sources.arianna.NewAriannaImporter -DB $db -orgFileArianna ${data_path}arianna/arianna1_soggetti_01dic17.csv -orgFileBusinessRegistry ${data_path}arianna/arianna_soggetti_01dic17_infocamere.csv -prjFile ${data_path}arianna/arianna_progetti_01dic17.csv -orgPrjFile '${data_path}arianna/raccordo soggetti progetti_19dic2017.csv' -entityType "all" &> ${log_dir}log_arianna_${db}.log";
#BVD
executor4="java -Xmx10g -cp ${project_jar} it.unimore.alps.sources.bvd.BvDImporter -DB $db -orgFile ${data_path}bvd/italy_companies.csv -financialFile ${data_path}bvd/italy_financial.csv -leaderFile ${data_path}bvd/italy_leaders.csv &> ${log_dir}log_bvd_${db}.log";
#CERCAUNIVERSITA
executor5="java -Xmx10g -cp ${project_jar} it.unimore.alps.sources.cercauniversita.CercaUniversitaImporter -DB $db -uniFile ${data_path}cercauniversita/AteneiPubPriv.csv -depFile ${data_path}cercauniversita/Dipartimenti.csv -instituteFile ${data_path}cercauniversita/Istituti.csv -centreFile ${data_path}cercauniversita/Centri.csv -fellowFile ${data_path}cercauniversita/Assegnisti.csv -profFile ${data_path}cercauniversita/Docenti.csv &> ${log_dir}log_cercauniversita_${db}.log";
#CNR
executor6="java -Xmx10g -cp ${project_jar} it.unimore.alps.sources.cnr.CnrImporter -DB $db -instituteFile ${data_path}cnr/istituto.json -departmentFile ${data_path}cnr/dipartimenti.json -parentCNRFile "${data_path}cnr/ParentCNR.csv" &> ${log_dir}log_cnr_${db}.log";
#PATIRIS
executor7="java -Xmx10g -cp ${project_jar} it.unimore.alps.sources.patiris.PatirisImporter -DB $db &> ${log_dir}log_patiris_${db}.log";
#QUESTIO
executor8="java -Xmx10g -cp ${project_jar} it.unimore.alps.sources.questio.QuestioImporter -DB $db -data ${data_path}questio/QuestioCrawledData.csv &> ${log_dir}log_questio_${db}.log";
#REGISTRO IMPRESE
executor9="java -Xmx10g -cp ${project_jar} it.unimore.alps.sources.registroimprese.RegistroImpreseImporter -DB $db -pmiFile ${data_path}registroimprese/pmi.csv -startupFile ${data_path}registroimprese/startup02072018.csv -incubatorFile ${data_path}registroimprese/incubatori.csv &> ${log_dir}log_registro_${db}.log";
#P3
executor10="java -Xmx10g -cp ${project_jar} it.unimore.alps.sources.p3.P3Importer -DB $db -prjFile ${data_path}p3/P3_GrantExport_with_abstracts.csv -pplFile ${data_path}p3/P3_PersonExport.csv -pubFile ${data_path}p3/P3_PublicationExport.csv -outFile ${data_path}p3/P3_GrantOutputDataExport.csv -collabFile ${data_path}p3/P3_CollaborationExport.csv -hasAbstract -entityType all &> ${log_dir}log_p3_${db}.log";
#GRID
executor11="java -Xmx10g -cp ${project_jar} it.unimore.alps.sources.grid.GridImporter -DB $db -orgFile ${data_path}grid/institutes.csv -aliasFile ${data_path}grid/aliases.csv -addressFile ${data_path}grid/addresses.csv -linkFile ${data_path}grid/links.csv -typeFile ${data_path}grid/types.csv -idFile ${data_path}grid/external_ids.csv -acronymFile ${data_path}grid/acronyms.csv -labelFile ${data_path}grid/labels.csv -relationshipFile ${data_path}grid/relationships.csv -geonameFile ${data_path}grid/geonames.csv -entityType organization &> ${log_dir}log_grid_${db}.log";
#FWF
executor12="java -Xmx10g -cp ${project_jar} it.unimore.alps.sources.fwf.FwfImporter -DB $db -projectFile ${data_path}fwf/fwf_projects.csv &> ${log_dir}log_fwf_${db}.log";
#SICRIS
executor13="java -Xmx10g -cp ${project_jar} it.unimore.alps.sources.sicris.SicrisImporter -DB $db -orgFile ${data_path}sicris/organizations.json -prjFile ${data_path}sicris/projects.json -intPrjFile ${data_path}sicris/international_projects.json -peopleFile ${data_path}sicris/people.json &> ${log_dir}log_sicris_${db}.log";
#ARAMIS
executor14="java -Xmx10g -cp ${project_jar} it.unimore.alps.sources.aramis.AramisImporter -DB $db -data ${data_path}aramis/AramisExport.csv &> ${log_dir}log_aramis_${db}.log";
#FOEN
executor15="java -Xmx10g -cp ${project_jar} it.unimore.alps.sources.foen.FoenImporter -DB $db -data ${data_path}foen/uid.admin.ch.json &> /home/matteop/log/log_foen_${db}.log";
#OPENAIRE
executor16="java -Xmx80g -cp ${project_jar} it.unimore.alps.sources.openaire.OpenAireImporter -orgFolder /home/paolos/openaire/organizations/ -orgPrefix out_ -country IT,FR,DE,AT,LI,SI,CH -prjFolder /home/paolos/openaire/projects/ -prjPrefix projects_ -orgFileH2020 /home/paolos/cordis/cordis-h2020organizations_mod.csv -orgFileFP7 /home/paolos/cordis/cordis-fp7organizations_mod.csv -prjFileH2020 /home/paolos/cordis/cordis-h2020projects.csv -prjFileFP7 /home/paolos/cordis/cordis-fp7projects.csv -db ${db} -pubFolderOpenAire /home/paolos/openaire/publications/ &> ${log_dir}logImporterOpenaire_${db}.log";
#GENERIC IMPORTER
executor17="java -Xmx10g -cp ${project_jar} it.unimore.alps.genericimporter.GenericImporter -DB ${db} -data /home/matteop/new_data/generic_data/general_data.csv &> ${log_dir}log_generic_importer_${db}.log";

executors=("$executor1" "$executor2" "$executor3" "$executor4" "$executor5" "$executor6" "$executor7" "$executor8" "$executor9" "$executor10" "$executor11" "$executor12" "$executor13" "$executor14" "$executor15" "$executor16" "$executor17");

echo "Execution of ${executors[${N}]}";
#Per ora non lo eseguiamo
#eval "${executors[${N}]}";
retval=$?;
if [ $retval -ne 0 ]; then
	echo "Error in importing phase.";
	exit 1;
fi
echo "Data import successfully completed.";
# END data import ---------------------------------------------------------------------------------
