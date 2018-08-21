
val PATTERN_HADOOP = """([0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])\s(\d{2}:\d{2}:\d{2}))(.*)\s(INFO|ERROR|WARN|TRACE|DEBUG|FATAL)\s\[(.*?)\]\s(.*\..*?:)\s(.*:{0,1})""".r
val PATTERN_APACHE = """(\[)(.*?\s.*\s[0-3][0-9]\s\d{2}:\d{2}:\d{2}\s\d{4})(\])(\s\[(.*?)\]\s)(.*)""".r
val PATTERN_BGL = """((-|\w*)\s\d*)\s(\d{4}.\d{2}.\d{2})(\s\w*-)*(.*?)\s(\d{4}.\d{2}.\d{2}.\d{2}.\d{2}.\d{2})(.\d{6})\s(.*)\s((INFO|ERROR|WARNING|TRACE|DEBUG|SEVERE|FATAL|FAILURE))($|\s(.*))""".r
val PATTERN_HDFS = """(\d*\s)*(INFO|ERROR|WARN|WARNING|TRACE|DEBUG|FATAL|SEVERE)\s(.*)""".r
val PATTERN_OPENSTACK = """(\d{4}[^a-zA-Z]*?)_(\d*[^a-zA-Z]*?)\s(\d*[^a-zA-Z]*)(INFO|ERROR|WARN|WARNING|TRACE|DEBUG|FATAL|SEVERE)\s(.*)""".r
val PATTERN_SPARK = """(\d{2}[^a-zA-Z]*)\s(INFO|ERROR|WARN|WARNING|TRACE|DEBUG|FATAL|SEVERE)\s(.*)""".r
val PATTERN_WINDOWS = """(\d{4}[^a-zA-Z]*),\s(Info|Error|Warn|Warning|Trace|Debug|Fatal|Severe)\s*(.*?)\s(\S.*)""".r
val PATTERN_ZOOKEEPER = """(\d{4}[^a-zA-Z]*)(,\d{3}[^a-zA-Z]*)(INFO|ERROR|WARN|WARNING|TRACE|DEBUG|FATAL|SEVERE)\s*(.*?)\s-\s(.*)""".r
val PATTERN_ANDROID = """(\d{2}[^a-zA-Z]*)(\.)(\d+\s*)*(I|E|W|T|D|F|S|V)\s(.*)""".r


import org.joda.time.{DateTimeZone}
import org.joda.time.format.DateTimeFormat

def formatTimestamp(timestamp: String) = {

	val df = sc.parallelize(Seq(timestamp)).toDF("dateCol")
	val modif = df.withColumn("Date",unix_timestamp(df.col("dateCol"), "EEE MMM dd HH:mm:ss yyyy"))
	val timeInMillisec = modif.select("Date")
	timeInMillisec.select(to_utc_timestamp(from_unixtime($"Date"),"yyyy-MM-dd hh:mm:ss").alias("timestamp"))
}

/*----------------------------------------------------------------------------------------------------------------*/


case class LogRecord(system: String, timestamp: String, SeverityLevel: String, SeverityIndex: String, message: String)

val leftQuote = "\u201C"
val rightQuote = "\u201D"

def parseLogLine(log: String): LogRecord = {
	
//  val result = pattern.findFirstMatchIn(log)
	// val result = PATTERN_HADOOP.findFirstMatchIn(log)
	// val result2 = PATTERN_APACHE.findFirstMatchIn(log)
	// val result3 = PATTERN_BGL.findFirstMatchIn(log)
	// val result4 = PATTERN_HDFS.findFirstMatchIn(log)
	// val result5 = PATTERN_OPENSTACK.findFirstMatchIn(log)
	// val result6 = PATTERN_SPARK.findFirstMatchIn(log)
	// val result7 = PATTERN_WINDOWS.findFirstMatchIn(log)
	val result8 = PATTERN_ZOOKEEPER.findFirstMatchIn(log)
	// val result9 = PATTERN_ANDROID.findFirstMatchIn(log)

   // if (result.isEmpty) {
   //     println("Rejected Log Line: " + log)
   //     LogRecord("Empty2", "-", "-", "-", "-")
   // }
   // else {
   //     val m = result.get
		 //  LogRecord( "Hadoop", m.group(1), m.group(6), "TODO", leftQuote+m.group(8)+m.group(9)+rightQuote)
   // }

  //  if (result2.isEmpty) {
  //      println("Rejected Log Line: " + log)
  //      LogRecord("Empty2", "-", "-", "-", "-")
  //  }
  //  else {
		// val n = result2.get
		// LogRecord( "Apache", n.group(2), n.group(5), "TODO", leftQuote+n.group(6)+rightQuote )
  //  }

	// if (result3.isEmpty) {
 //   	println("Rejected Log Line: " + log)
 //   	LogRecord("Empty2", "-", "-", "-", "-")
	// }
	// else {
 //   	val m = result3.get
	// 	LogRecord( "BGL/P", m.group(6), m.group(9), "TODO", leftQuote+m.group(12)+rightQuote)
	// }
	
	// if (result4.isEmpty) {
 //   	println("Rejected Log Line: " + log)
 //   	LogRecord("Empty2", "-", "-", "-", "-")
	// }
	// else {
 //   	val m = result4.get
	// 	LogRecord( "HDFS", "N/A for the moment", m.group(2), "TODO", leftQuote+m.group(3)+rightQuote)
	// }
	
	// if (result5.isEmpty) {
 //   	println("Rejected Log Line: " + log)
 //   	LogRecord("Empty2", "-", "-", "-", "-")
	// }
	// else {
 //   	val m = result5.get
	// 	LogRecord( "OpenStack", m.group(1)+" "+ m.group(2), m.group(4), "TODO", leftQuote+m.group(5)+rightQuote)
	// }

	// if (result6.isEmpty) {
	// 	println("Rejected Log Line: " + log)
	// 	LogRecord("Empty2", "-", "-", "-", "-")
	// }
	// else {
	// 	val m = result6.get
	// 	LogRecord( "Spark", m.group(1), m.group(2), "TODO", leftQuote+m.group(3)+rightQuote )
	// }
	
	// if (result7.isEmpty) {
	// 	println("Rejected Log Line: " + log)
	// 	LogRecord("Empty2", "-", "-", "-", "-")
	// }
	// else {
	// 	val m = result7.get
	// 	LogRecord( "Windows", m.group(1), m.group(2), "TODO", leftQuote+m.group(4)+rightQuote)
	// }
	
	if (result8.isEmpty) {
		println("Rejected Log Line: " + log)
		LogRecord("Empty2", "-", "-", "-", "-")
	}
	else {
		val m = result8.get
		LogRecord( "Zookeeper", m.group(1), m.group(3), "TODO", leftQuote+m.group(5)+rightQuote)
	}

	// if (result9.isEmpty) {
	// 	println("Rejected Log Line: " + log)
	// 	LogRecord("Empty2", "-", "-", "-", "-")
	// }
	// else {
	// 	val m = result9.get
	// 	LogRecord( "Android", m.group(1), m.group(4), "TODO", leftQuote+m.group(5)+rightQuote)
	// }

}

// val accessLogs = sc.textFile("/Users/vaati/Desktop/loghub/Hadoop/Hadoop_2k.log").map(parseLogLine).toDF()
// val accessLogs = sc.textFile("/Users/vaati/Desktop/LogHub Datasets/Apache/Apache.log").map(parseLogLine).toDF()
// val accessLogs = sc.textFile("/Users/vaati/Desktop/LogHub Datasets/BGL/BGL.log").map(parseLogLine).toDF()
// val accessLogs = sc.textFile("/Users/vaati/Desktop/LogHub Datasets/HDFS/HDFS.log").map(parseLogLine).toDF()
// val accessLogs = sc.textFile("/Users/vaati/Desktop/loghub/OpenStack/OpenStack_2k.log").map(parseLogLine).toDF()
// val accessLogs = sc.textFile("/Users/vaati/Desktop/loghub/Spark/Spark_2k.log").map(parseLogLine).toDF()
// val accessLogs = sc.textFile("/Users/vaati/Desktop/loghub/Windows/Windows_2k.log").map(parseLogLine).toDF()
val accessLogs = sc.textFile("/Users/vaati/Desktop/LogHub Datasets/Zookeeper/Zookeeper.log").map(parseLogLine).toDF()
// val accessLogs = sc.textFile("/Users/vaati/Desktop/loghub/Android/Android_2k.log").map(parseLogLine).toDF()

accessLogs.show()

// accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").save("/Users/vaati/Desktop/loghub/Hadoop/Hadoop.csv")
// accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").option("quote", "\u0000").save("/Users/vaati/Desktop/LogHub Datasets/Apache/Apache.csv")
// accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").option("quote", "\u0000").save("/Users/vaati/Desktop/LogHub Datasets/BGL/BGL.csv")
// accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").option("quote", "\u0000").save("/Users/vaati/Desktop/LogHub Datasets/HDFS/HDFS.csv")
// accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").option("quote", "\u0000").save("/Users/vaati/Desktop/loghub/OpenStack/OpenStack.csv")
// accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").option("quote", "\u0000").save("/Users/vaati/Desktop/loghub/Spark/Spark.csv")
// accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").option("quote", "\u0000").save("/Users/vaati/Desktop/loghub/Windows/Windows.csv")
accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").option("quote", "\u0000").save("/Users/vaati/Desktop/LogHub Datasets/Zookeeper/Zookeeper.csv")
// accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").option("quote", "\u0000").save("/Users/vaati/Desktop/loghub/Android/Android.csv")


// val dataset = spark.read.option("delimiter", ";").csv("/Users/vaati/Desktop/LogHub Datasets/Apache/Apache.csv/part-00000-8a749eb0-46d2-471d-b720-02fcd3ebb016-c000.csv").toDF
// val modif = dataset.withColumn("Date",unix_timestamp(dataset.col("_c1"), "EEE MMM dd HH:mm:ss yyyy")).drop("_c1")
// val tsar = modif.withColumn("Date", to_utc_timestamp(from_unixtime($"Date"),"yyyy-MM-dd hh:mm:ss")).alias("timestamp")
// tsar.show
// val new_df = tsar.select("_c0", "Date", "_c2", "_c3", "_c4")
// new_df.show

// new_df.write.format("com.databricks.spark.csv").option("delimiter",";").option("header",true).option("inferSchema","true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").save("/Users/vaati/Desktop/LogHub Datasets/Apache/ApacheFormatted.csv")

