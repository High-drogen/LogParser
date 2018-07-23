import scala.util.matching.Regex

val PATTERN_ANDROID = """(\d{2}[^a-zA-Z]*)\s(I|E|W|T|D|F|S|V)\s(.*)""".r

case class LogRecord(system: String, timestamp: String, SeverityLevel: String, message: String)

def parseLogLine(pattern: Regex): ( String => LogRecord) = {

	log => {
		val result9 = PATTERN_ANDROID.findFirstMatchIn(log)
		
		println(Console.YELLOW + "NOTICE THAT LOG : " + log + Console.WHITE)
		println(Console.GREEN + "NOTICE THAT PATTERN : " + pattern + Console.WHITE)
		
		if (result9.isEmpty) {
			println("Rejected Log Line: " + log)
			LogRecord("Empty2", "-", "-", "")
		}
		else {
			val m = result9.get
			LogRecord( "Android", m.group(1), m.group(2), m.group(3))
		}
	}
}

val logData = sc.textFile("/Users/vaati/Desktop/loghub/Andriod/Andriod_2k.log")

println(Console.RED + "TYPE OF LOGDATA : " + logData.getClass + Console.WHITE )

def accessLogs = logData.map( parseLogLine(PATTERN_ANDROID) ).toDF()

accessLogs.show()

//accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").save("/Users/vaati/Desktop/loghub/Andriod/Android.csv")