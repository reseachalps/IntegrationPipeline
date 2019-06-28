@Library('re-search-alps-library')_
pipeline {
    agent any
    parameters{
        string(name: 'project_jar', defaultValue: 'prova.jar', description: 'File java da usare nel progetto')
        string(name: 'data_path', defaultValue: '/home/matteop/new_data/', description: 'Nome assoluto da usare')
        string(name: 'db', defaultValue: 'Test', description: 'Nome da utilizzare nella creazione del database')
        choice(name: 'source', choices: ['0 ORCID', '1 SCANR', '2 ARIANNA', '3 BVD', '4 CERCAUNIVERSITA', '5 CNR', '6 PATIRIS', '7 QUESTIO', '8 REGISTRO IMPRESE', '9 P3', '10 GRID', '11 FWF', '12 SICRIS', '13 ARAMIS', '14 FOEN', '15 OPENAIRE', '16 GENERIC IMPORTER'], description: 'Scegli fonte da utilizzare')
    }
    environment{
        lastState = readFile('log.txt').trim()
    }
    stages {
        stage('Importazione'){
            steps{
                script{
                    for (int i = 0; i < 17; i++) {
                        importer("${project_jar}", "${data_path}", "${db}", "${i}")
                    }
                }
            }
        }
        stage('SingolaImportazione'){
            steps{
                importer("${project_jar}", "${data_path}", "${db}", "${choice[0]}")
            }
            post {
                success {
                    echo "Fase importazione completata con successo"
                    sh "echo import > log.txt"
                    echo "${lastState}"
                }
            }
        }
        stage('Integrazione'){
            steps{
                script{
                    if("${lastState}" == "import"){
                        echo "Siamo liberi di proseguire"
                    }
                }
            }
        }
        stage('EsportazioneDecuplicazione'){
            steps{
                script{
                     if("${lastState}" == "integration"){
                       echo "Siamo liberi di proseguire"
                   }
                  else
                  {
                      input(message: "Attenzione, sicuro di voler proseguire?", ok: "Va bene")
                      echo "Eseguiamo comunque"
//                      error 'Non possiamo proseguire'
                  }
              }
            }
        }
    }
}
