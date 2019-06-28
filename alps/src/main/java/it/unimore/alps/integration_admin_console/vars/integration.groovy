def call (String project_jar, String db, String log_dir) {
	def integration = libraryResource'integration.sh'
	writeFile file: 'integration.sh', text: integration
	sh 'chmod +x integration.sh'
	echo "Parameters:\nfile jar: ${project_jar}\ndb: ${db}\nlog_dir: ${log_dir}"
	rc = sh(script: "./integration.sh ${project_jar} ${db} ${log_dir}", returnStatus: true)
	if (rc == 1)
		error 'importer script failed'
	else
		if (rc == 2)
			error 'bad parameters'
}
