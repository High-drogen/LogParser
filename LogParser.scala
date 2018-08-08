val PATTERN_ALL = """([0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])\s(\d{2}:\d{2}:\d{2}))(.*)\s(INFO|ERROR|WARN|TRACE|DEBUG|FATAL)\s\[(.*?)\]\s(.*\..*?:)\s(.*:{0,1})""".r
val PATTERN_APACHE = """(\[(.*?)\s(.*)\s[0-3][0-9]\s(\d{2}:\d{2}:\d{2})\s\d{4}\])(\s\[(.*?)\]\s)(.*)""".r
val PATTERN_BGL = """((-|\w*)\s\d*)\s(\d{4}.\d{2}.\d{2})(\s\w*-)*(.*?)\s(\d{4}.\d{2}.\d{2}.\d{2}.\d{2}.\d{2}.\d{6})\s(.*)\s(INFO|ERROR|WARNING|TRACE|DEBUG|SEVERE|FATAL|FAILURE)(.*)""".r
val PATTERN_HDFS = """(\d*\s)*(INFO|ERROR|WARN|WARNING|TRACE|DEBUG|FATAL|SEVERE)\s(.*)""".r
val PATTERN_OPENSTACK = """((\d{4}[^a-zA-Z]*)\s\d*)\s(INFO|ERROR|WARN|WARNING|TRACE|DEBUG|FATAL|SEVERE)\s(.*)""".r
val PATTERN_SPARK = """(\d{2}[^a-zA-Z]*)\s(INFO|ERROR|WARN|WARNING|TRACE|DEBUG|FATAL|SEVERE)\s(.*)""".r
val PATTERN_WINDOWS = """(\d{4}[^a-zA-Z]*)\s(Info|Error|Warn|Warning|Trace|Debug|Fatal|Severe)\s*(.*?)\s(.*)""".r
val PATTERN_ZOOKEEPER = """(\d{4}[^a-zA-Z]*)\s(INFO|ERROR|WARN|WARNING|TRACE|DEBUG|FATAL|SEVERE)\s*(.*?)\s(.*)""".r
val PATTERN_ANDROID = """(\d{2}[^a-zA-Z]*)\s(I|E|W|T|D|F|S|V)\s(.*)""".r

case class LogRecord(system: String, timestamp: String, SeverityLevel: String, message: String)

def parseLogLine(log: String): LogRecord = {
	
//  val result = pattern.findFirstMatchIn(log)
//	val result = PATTERN_ALL.findFirstMatchIn(log)
//	val result2 = PATTERN_APACHE.findFirstMatchIn(log)
//	val result3 = PATTERN_BGL.findFirstMatchIn(log)
//	val result4 = PATTERN_HDFS.findFirstMatchIn(log)
//	val result5 = PATTERN_OPENSTACK.findFirstMatchIn(log)
//	val result6 = PATTERN_SPARK.findFirstMatchIn(log)
	val result7 = PATTERN_WINDOWS.findFirstMatchIn(log)
//	val result8 = PATTERN_ZOOKEEPER.findFirstMatchIn(log)
//	val result9 = PATTERN_ANDROID.findFirstMatchIn(log)

//    if (result.isEmpty) {
//        println("Rejected Log Line: " + log)
//        LogRecord("Empty2", "-", "-", "")
//    }
//    else {
//        val m = result.get
//		  LogRecord( "Hadoop", m.group(1), m.group(6), m.group(8)+""+m.group(9))
//    }

//    if (result2.isEmpty) {
//        println("Rejected Log Line: " + log)
//        LogRecord("Empty2", "-", "-", "")
//    }
//    else {
//		val n = result2.get
//		LogRecord( "Apache", n.group(1), n.group(6), n.group(7) )
//    }

//	if (result3.isEmpty) {
//    	println("Rejected Log Line: " + log)
//    	LogRecord("Empty2", "-", "-", "")
//	}
//	else {
//    	val m = result3.get
//		LogRecord( "BGL/P", m.group(6), m.group(8), m.group(9))
//	}
	
//	if (result4.isEmpty) {
//    	println("Rejected Log Line: " + log)
//    	LogRecord("Empty2", "-", "-", "")
//	}
//	else {
//    	val m = result4.get
//		LogRecord( "HDFS", "N/A for the moment", m.group(2), m.group(3))
//	}
	
//	if (result5.isEmpty) {
//    	println("Rejected Log Line: " + log)
//    	LogRecord("Empty2", "-", "-", "")
//	}
//	else {
//    	val m = result5.get
//		LogRecord( "OpenStack", m.group(2), m.group(3), m.group(4))
//	}

//	if (result6.isEmpty) {
//		println("Rejected Log Line: " + log)
//		LogRecord("Empty2", "-", "-", "")
//	}
//	else {
//		val m = result6.get
//		LogRecord( "Spark", m.group(1), m.group(2), m.group(3))
//	}
	
	if (result7.isEmpty) {
		println("Rejected Log Line: " + log)
		LogRecord("Empty2", "-", "-", "")
	}
	else {
		val m = result7.get
		LogRecord( "Windows", m.group(1), m.group(2), m.group(4))
	}
	
//	if (result8.isEmpty) {
//		println("Rejected Log Line: " + log)
//		LogRecord("Empty2", "-", "-", "")
//	}
//	else {
//		val m = result8.get
//		LogRecord( "Zookeeper", m.group(1), m.group(2), m.group(4))
//	}

//	if (result9.isEmpty) {
//		println("Rejected Log Line: " + log)
//		LogRecord("Empty2", "-", "-", "")
//	}
//	else {
//		val m = result9.get
//		LogRecord( "Android", m.group(1), m.group(2), m.group(3))
//	}

}

//val accessLogs = sc.textFile("/Users/vaati/Desktop/loghub/Hadoop/Hadoop_2k.log").map(parseLogLine).toDF()
//val accessLogs = sc.textFile("/Users/vaati/Desktop/LogHub Datasets/Apache/Apache.log").map(parseLogLine).toDF()
//val accessLogs = sc.textFile("/Users/vaati/Desktop/LogHub Datasets/BGL/BGL.log").map(parseLogLine).toDF()
//val accessLogs = sc.textFile("/Users/vaati/Desktop/LogHub Datasets/HDFS/HDFS.log").map(parseLogLine).toDF()
//val accessLogs = sc.textFile("/Users/vaati/Desktop/loghub/OpenStack/OpenStack_2k.log").map(parseLogLine).toDF()
val accessLogs = sc.textFile("/Users/vaati/Desktop/loghub/Spark/Spark_2k.log").map(parseLogLine).toDF()
val accessLogs = sc.textFile("/Users/vaati/Desktop/loghub/Windows/Windows_2k.log").map(parseLogLine).toDF()
//val accessLogs = sc.textFile("/Users/vaati/Desktop/LogHub Datasets/Zookeeper/Zookeeper.log").map(parseLogLine).toDF()
//val accessLogs = sc.textFile("/Users/vaati/Desktop/loghub/Android/Android_2k.log").map(parseLogLine).toDF()

accessLogs.show()

//accessLogs.write.format("com.databricks.spark.csv").save("/Users/vaati/Desktop/loghub/Hadoop/Hadoop.csv")
//accessLogs.write.format("com.databricks.spark.csv").save("/Users/vaati/Desktop/LogHub Datasets/Apache/Apache.csv")
//accessLogs.write.format("com.databricks.spark.csv").save("/Users/vaati/Desktop/LogHub Datasets/BGL/BGL.csv")
//accessLogs.write.format("com.databricks.spark.csv").save("/Users/vaati/Desktop/LogHub Datasets/HDFS/HDFS.csv")
//accessLogs.write.format("com.databricks.spark.csv").save("/Users/vaati/Desktop/loghub/OpenStack/OpenStack.csv")
//accessLogs.write.format("com.databricks.spark.csv").save("/Users/vaati/Desktop/loghub/Spark/Spark.csv")
accessLogs.write.format("com.databricks.spark.csv").save("/Users/vaati/Desktop/loghub/Windows/Windows.csv")
//accessLogs.write.format("com.databricks.spark.csv").save("/Users/vaati/Desktop/LogHub Datasets/Zookeeper/Zookeeper.csv")
//accessLogs.write.format("com.databricks.spark.csv").option("delimiter","|").save("/Users/vaati/Desktop/loghub/Android/Android.csv")
