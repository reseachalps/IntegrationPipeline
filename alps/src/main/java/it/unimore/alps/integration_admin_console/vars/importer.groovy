def call (String project_jar, String data_path, String db, String log_dir, String N) {
	def importer = libraryResource'importer.sh'
	writeFile file: 'importer.sh', text: importer
	sh 'chmod +x importer.sh'
	rc = sh(script: "./importer.sh ${project_jar} ${data_path} ${db} ${log_dir} ${N}", returnStatus: true)
	if (rc == 1)
		error 'importer script failed'
	else
		if (rc == 2)
			error 'bad parameters'
}
