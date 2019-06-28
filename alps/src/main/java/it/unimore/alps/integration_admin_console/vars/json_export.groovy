def call (String project_jar, String db, String log_dir, String dedup_db) {
	def json_export = libraryResource'json_export.sh'
	writeFile file: 'json_export.sh', text: json_export
	sh 'chmod +x json_export.sh'
	echo "Parameters:\nfile jar: ${project_jar}\ndb: ${db}\nlog_dir: ${log_dir}\ndedup_db: ${dedup_db}"
	rc = sh(script: "./json_export.sh ${project_jar} ${db} ${log_dir} ${dedup_db}", returnStatus: true)
	if (rc == 1)
		error 'importer script failed'
	else
		if (rc == 2)
			error 'bad parameters'
}
