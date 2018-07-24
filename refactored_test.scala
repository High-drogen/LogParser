import scala.util.matching.Regex
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._ 

// ----- THIS WILL BE CONTAINED IN AN EXTERNAL FILE -----------------------------------------------------------------------------

//val PATTERN = """(\d{2}[^a-zA-Z]*)\s(I|E|W|T|D|F|S|V)\s(.*)""".r

val sqlContext = new SQLContext(sc)
val df = sqlContext.read.option("multiline", true).json("/Users/vaati/Desktop/LogParser/LogParser/info.json")
val child = df.select(df("ANDROID.pattern")).toDF("pattern")


println(Console.RED + child.getClass + Console.WHITE)


val thePattern = child.select(child("pattern"))
thePattern.show()
val DFtoProcess = sqlContext.sql("SELECT * FROM pattern")
//val str: String = thePattern.mkString()



//val regexStr: String = child.select(child("pattern")).toString()

//println("CLASS OF REGEX " + regexStr.getClass)
//println( Console.RED + "STRING CONTENT: " + regexStr + Console.WHITE )

//val strrr = "Something"
//println( Console.RED + strrr + Console.WHITE )
//println( Console.RED + strrr.getClass + Console.WHITE )

//val PATTERN = new Regex(regexStr)

//println( Console.RED + "STRING CONTENT: " + PATTERN + Console.WHITE )

// ------------------------------------------------------------------------------------------------------------------------------
/*
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
*/
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