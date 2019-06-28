def call (){
  String lastState
  try{
    lastState = readFile('log.txt').trim()
  } catch (Exception e) {
    sh 'touch log.txt'
    lastState = ""
  }
  return lastState
}
