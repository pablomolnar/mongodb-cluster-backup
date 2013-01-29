@Grab( group='org.mongodb', module='mongo-java-driver', version='2.10.1')

import com.mongodb.*

assert args && args[0], "No server to connect was given. Usage: backup 127.0.0.1:27017"

println "----------------"
println "- Start backup -"
println "----------------"
println ""

def date = new Date().format('yyyy-MM-dd-HH-mm-ss')
def backupFolder = "/tmp/backup/$date"  

def mongo = new Mongo(args[0])
def configdb = mongo.getDB("admin").command('getCmdLineOpts').parsed.configdb
def replicas = mongo.getDB("config").getCollection("shards").find().collect {
  def rsSetHosts = it.host.split("/")[1].split(",")
  new Mongo(rsSetHosts[0]).getDB("admin").command('replSetGetStatus').members.findAll{ it.stateStr == "SECONDARY"}.max { it.optime }.name
}

println "Settings:"
println "mongos   -> ${args[0]}"
println "configdb -> $configdb"
println "replicas -> $replicas"

try {
  lnprintln "1 Step - Stop Balancer"
  stopBalancer()

  lnprintln "2 Step - Lock one member of each replica set in each shard"
  replicas.each {
      def result = new Mongo(it).getDB("admin").command(new BasicDBObject([fsync:1, lock:true]))
      println "Locking $it: $result"
  }

  lnprintln "3 Step - Backup mongo config server"
  println "mongodump --db config --host $configdb --out $backupFolder".execute().text

  lnprintln "4 Step - Back up the locked replica set members"
  replicas.each {
    //curl -X POST api.melicloud.com/storage/snapshots -H "x-auth-token: TOKEN" -d '{"label":"LABEL", "instance": "INSTANCE"}'
  }

  lnprintln "5 Step - Unlock replica set mebers"
  replicas.each {
      def result = new Mongo(it).getDB("admin").getCollection('$cmd.sys.unlock').findOne()
      println "Unlocking $it: $result"
  }

} catch(e) {
  lnprintln "ERRORS:"
  e.printStackTrace()

} finally {

  lnprintln "6 Step - Start Balancer"
  startBalancer()

}


lnprintln "Backup ended!"



def lnprintln(String str) { println "\n" + str }

// Balancer Methods
def startBalancer() { setBalancerSate(true) }
def stopBalancer() { setBalancerSate(false) }

def setBalancerSate(value) {  
  println "Setting balancer state to: $value"
    
  def mongo = new Mongo(args[0])
  def configDB = mongo.getDB("config")
  def settings = configDB.getCollection("settings")
  
  BasicDBObject set = new BasicDBObject('$set', new BasicDBObject("stopped", !value))
  settings.findAndModify(new BasicDBObject("_id", "balancer"), null, null, false, set, true, true)
  
  if (!value) {
  	def locks = configDB.getCollection("locks")
    while (locks.findOne(new BasicDBObject("_id", "balancer"))?.state) {
		println("Balancer still active...")
		sleep(1000)
    }
  }
  
  println "Balancer status set"
}