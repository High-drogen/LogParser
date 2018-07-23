import scala.util.matching.Regex
import org.apache.spark.sql.SQLContext

// ----- THIS WILL BE CONTAINED IN AN EXTERNAL FILE -----------------------------------------------------------------------------

val PATTERN = """(\d{2}[^a-zA-Z]*)\s(I|E|W|T|D|F|S|V)\s(.*)""".r
val PATTERN_ALL = """([0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])\s(\d{2}:\d{2}:\d{2}))(.*)\s(INFO|ERROR|WARN|TRACE|DEBUG|FATAL)\s\[(.*?)\]\s(.*\..*?:)\s(.*:{0,1})""".r
val PATTERN_APACHE = """(\[(.*?)\s(.*)\s[0-9][1-9]\s(\d{2}:\d{2}:\d{2})\s\d{4}\])(\s\[(.*?)\]\s)(.*)""".r
val PATTERN_BGL = """((-|\w*)\s\d*)\s(\d{4}.\d{2}.\d{2})(\s\w*-)*(.*?)\s(\d{4}.\d{2}.\d{2}.\d{2}.\d{2}.\d{2}.\d{6})\s(.*)\s(INFO|ERROR|WARNING|TRACE|DEBUG|FATAL)\s(.*)""".r
val PATTERN_HDFS = """(\d*\s)*(INFO|ERROR|WARN|WARNING|TRACE|DEBUG|FATAL|SEVERE)\s(.*)""".r
val PATTERN_OPENSTACK = """((\d{4}[^a-zA-Z]*)\s\d*)\s(INFO|ERROR|WARN|WARNING|TRACE|DEBUG|FATAL|SEVERE)\s(.*)""".r
val PATTERN_SPARK = """(\d{2}[^a-zA-Z]*)\s(INFO|ERROR|WARN|WARNING|TRACE|DEBUG|FATAL|SEVERE)\s(.*)""".r
val PATTERN_WINDOWS = """(\d{4}[^a-zA-Z]*)\s(Info|Error|Warn|Warning|Trace|Debug|Fatal|Severe)\s*(.*?)\s(.*)""".r
val PATTERN_ZOOKEEPER = """(\d{4}[^a-zA-Z]*)\s(INFO|ERROR|WARN|WARNING|TRACE|DEBUG|FATAL|SEVERE)\s*(.*?)\s(.*)""".r
// val PATTERN = "" // TODO: get it from JSON file

val reviewDF = spark.read.option("multiline", true).json("/Users/vaati/Desktop/LogParser/LogParser/info.json").printSchema()

// ------------------------------------------------------------------------------------------------------------------------------

case class LogRecord(system: String, timestamp: String, SeverityLevel: String, message: String)

def parseLogLine(pattern: Regex): ( String => LogRecord) = {

	log => {
		val result = pattern.findFirstMatchIn(log)
		
		if (result.isEmpty) {
			println("Rejected Log Line: " + log)
			LogRecord("Empty2", "-", "-", "")
		}
		else {
			val m = result.get
			LogRecord( "Android", m.group(1), m.group(2), m.group(3))
		}
	}
}

val logData = sc.textFile("/Users/vaati/Desktop/loghub/Andriod/Andriod_2k.log")
//val accessLogs = sc.textFile("/Users/vaati/Desktop/loghub/Hadoop/Hadoop_2k.log")
//val logData = sc.textFile("/Users/vaati/Desktop/loghub/Apache/Apache_2k.log")
//val logData = sc.textFile("/Users/vaati/Desktop/loghub/BGL/BGL_2k.log")
//val logData = sc.textFile("/Users/vaati/Desktop/loghub/HDFS/HDFS_2k.log")
//val logData = sc.textFile("/Users/vaati/Desktop/loghub/OpenStack/OpenStack_2k.log")
//val logData = sc.textFile("/Users/vaati/Desktop/loghub/Spark/Spark_2k.log")
//val logData = sc.textFile("/Users/vaati/Desktop/loghub/Windows/Windows_2k.log")
//val logData = sc.textFile("/Users/vaati/Desktop/loghub/Zookeeper/Zookeeper_2k.log")
// val logData = sc.textFile(LOG_PATH) // TODO: get it from JSON file

def accessLogs = logData.map( parseLogLine(PATTERN) ).toDF()

accessLogs.show()

//accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").save("/Users/vaati/Desktop/loghub/Andriod/Android.csv")
//accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").save("/Users/vaati/Desktop/loghub/Hadoop/Hadoop.csv")
//accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").save("/Users/vaati/Desktop/loghub/Apache/Apache.csv")
//accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").save("/Users/vaati/Desktop/loghub/BGL/BGL.csv")
//accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").save("/Users/vaati/Desktop/loghub/HDFS/HDFS.csv")
//accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").save("/Users/vaati/Desktop/loghub/OpenStack/OpenStack.csv")
//accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").save("/Users/vaati/Desktop/loghub/Spark/Spark.csv")
//accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").save("/Users/vaati/Desktop/loghub/Windows/Windows.csv")
//accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").save("/Users/vaati/Desktop/loghub/Zookeeper/Zookeeper.csv")
// accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").save(CSV_PATH) // TODO: get it from JSON file