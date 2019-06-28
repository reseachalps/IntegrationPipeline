def call (String project_jar, String db, String log_dir, String dedup_db) {
	def nuts_integration = libraryResource'nuts_integration.sh'
	writeFile file: 'nuts_integration.sh', text: nuts_integration
	sh 'chmod +x nuts_integration.sh'
	echo "Parameters:\nfile jar: ${project_jar}\ndb: ${db}\nlog_dir: ${log_dir}\ndedup_db: ${dedup_db}"
	rc = sh(script: "./nuts_integration.sh ${project_jar} ${db} ${log_dir} ${dedup_db}", returnStatus: true)
	if (rc == 1)
		error 'importer script failed'
	else
		if (rc == 2)
			error 'bad parameters'
}
