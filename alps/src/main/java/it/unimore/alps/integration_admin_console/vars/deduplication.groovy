def call (String db, String log_dir, String path_csv_export) {
	def deduplication = libraryResource'deduplication.sh'
	writeFile file: 'deduplication.sh', text: deduplication
	sh 'chmod +x deduplication.sh'
	echo "Parameters:\ndb: ${db}\nlog_dir: ${log_dir}\npath_cvs_export: ${path_csv_export}"
	rc = sh(script: "./deduplication.sh ${db} ${log_dir} ${path_csv_export}", returnStatus: true)
	if (rc == 1)
		error 'importer script failed'
	else
		if (rc == 2)
			error 'bad parameters'
}
