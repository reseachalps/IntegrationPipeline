def error(String phase){
	emailext body: "Build failed during stage: ${phase}\n\nCheck console output at $BUILD_URL to view details", subject: '$DEFAULT_SUBJECT', to: '$DEFAULT_RECIPIENTS'
}

def success(){
	emailext body: '$DEFAULT_CONTENT', subject: '$DEFAULT_SUBJECT', to: '$DEFAULT_RECIPIENTS'
}
