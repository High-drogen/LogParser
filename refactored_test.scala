import scala.util.matching.Regex
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._ 

// -------------------------------------------------------------------------------------------------------------------------------

val sqlContext = new SQLContext(sc)
val df = sqlContext.read.option("multiline", true).json("/Users/vaati/Desktop/LogParser/LogParser/info.json").toDF()
df.registerTempTable("tempSmthg")
val smthg = sqlContext.sql("SELECT * FROM tempSmthg")
smthg.show
smthg.printSchema

val child_pattern = df.select("info")
child_pattern.registerTempTable("tempcust")
val pattern = sqlContext.sql("SELECT info.system FROM tempcust").first()

//val PATTERN = pattern.getString(0).r


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

def accessLogs = logData.map( parseLogLine(PATTERN) ).toDF()

accessLogs.show()

//accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").save("/Users/vaati/Desktop/loghub/Andriod/Android.csv")
*/