def call (String project_jar, String db, String log_dir, String path_csv_export) {
	def csv_export = libraryResource'csv_export.sh'
	writeFile file: 'csv_export.sh', text: csv_export
	sh 'chmod +x csv_export.sh'
	echo "Parameters:\nfile jar: ${project_jar}\ndb: ${db}\nlog_dir: ${log_dir}\npath_cvs_export: ${path_csv_export}"
	rc = sh(script: "./csv_export.sh ${project_jar} ${db} ${log_dir} ${path_csv_export}", returnStatus: true)
	if (rc == 1)
		error 'importer script failed'
	else
		if (rc == 2)
			error 'bad parameters'
}
