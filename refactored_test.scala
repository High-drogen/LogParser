import scala.util.matching.Regex
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.Row
import spark.implicits._
import org.apache.spark.rdd.RDD

import org.joda.time.{DateTimeZone}
import org.joda.time.format.DateTimeFormat

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

def findNull(row:Row):String = {
	if (row.getString(2) == "V") {
		"10"
	}
	else if (row.getString(2) == "INFO" || row.getString(2) == "info" || row.getString(2) == "notice" || row.getString(2) == "Info" || row.getString(2) == "I" ) {
		"30"
	}
	else if(row.getString(2) == "ERROR" || row.getString(2) == "error" || row.getString(2) == "Error" || row.getString(2) == "E" ){
		"50"
	}
	else if(row.getString(2) == "WARNING" || row.getString(2) == "warning" || row.getString(2) == "Warning" || row.getString(2) == "W" || row.getString(2) == "WARN" ){
		"40"
	}
	else if(row.getString(2) == "DEBUG" || row.getString(2) == "debug" || row.getString(2) == "Debug" || row.getString(2) == "D" ){
		"10"
	}
	else if(row.getString(2) == "FATAL" || row.getString(2) == "fatal" || row.getString(2) == "Fatal" || row.getString(2) == "F" ){
		"60"
	}
	else if(row.getString(2) == "SEVERE" || row.getString(2) == "severe" || row.getString(2) == "Severe" || row.getString(2) == "S" ){
		"60"
	}
	else if(row.getString(2) == "FAILURE" || row.getString(2) == "failure" || row.getString(2) == "Failure" ){
		"70"
	}
	else if(row.getString(2) == "C") {
		"80"
	}
	else {
		"??"
	}
}

spark.sqlContext.udf.register("findNull", findNull _)

//--------------

val leftQuote = "\u201C"
val rightQuote = "\u201D"

val sqlContext = new SQLContext(sc)
val df = sqlContext.read.option("multiline", true).json("/Users/vaati/Desktop/LogParser/LogParser/info.json").toDF
val df2 = df.withColumn("info", explode($"info")).withColumn("system", $"info"(0)).withColumn("pattern", $"info"(1)).withColumn("input", $"info"(2)).withColumn("output", $"info"(3)).withColumn("formatDate", $"info"(4)).drop("info").withColumn("id",monotonicallyIncreasingId)
df2.show()
df2.registerTempTable("mytable")
df2.count()

var valeur = df2.count() - 1

case class LogRecord(system: String, timestamp: String, SeverityLevel: String, SeverityIndex: String, message: String)

while(valeur != -1) {
	println("Index Variable value : " + valeur)

	val kaka = sqlContext.sql("select * from mytable where id = "+valeur+" ")
	
	val systName = kaka.select($"system").collectAsList().get(0)(0).toString

	val patternName = kaka.select($"pattern").collectAsList().get(0)(0).toString
	val inputName = kaka.select($"input").collectAsList().get(0)(0).toString
	val outputName = kaka.select($"output").collectAsList().get(0)(0).toString
	val dateFormatName = kaka.select($"formatDate").collectAsList().get(0)(0).toString

	println("Name : " + systName)
	println("Name : " + patternName)
	println("Name : " + inputName)
	println("Name : " + outputName)
	println("Name : " + dateFormatName)

	val PATTERN = patternName.r

	def parseLogLine(pattern: Regex): ( String => LogRecord) = {

		log => {
			val result = pattern.findFirstMatchIn(log)
			
			if (result.isEmpty) {
				println("Rejected Log Line: " + log)
				LogRecord("Empty2", "-", "-", "-", "-")
			}
			else {
				val m = result.get
				if(systName == "HADOOP"){
					LogRecord( systName, m.group(1), m.group(6), "TODO", leftQuote+m.group(8)+m.group(9)+rightQuote)
				} else if(systName == "APACHE") {
					LogRecord( systName, m.group(2), m.group(5), "TODO", leftQuote+m.group(6)+rightQuote )
				} else if(systName == "BGL") {
					LogRecord( systName, m.group(6), m.group(9), "TODO", leftQuote+m.group(12)+rightQuote)
				} else if(systName == "HDFS") {
					LogRecord( systName, "N/A for the moment", m.group(2), "TODO", leftQuote+m.group(3)+rightQuote)
				} else if(systName == "OPENSTACK") {
					LogRecord( systName, m.group(1)+" "+ m.group(2), m.group(4), "TODO", leftQuote+m.group(5)+rightQuote)
				} else if(systName == "SPARK") {
					LogRecord( systName, m.group(1), m.group(2), "TODO", leftQuote+m.group(3)+rightQuote )
				} else if(systName == "WINDOWS") {
					LogRecord( systName, m.group(1), m.group(2), "TODO", leftQuote+m.group(4)+rightQuote)
				} else if(systName == "ANDROID") {
					LogRecord( systName, m.group(1), m.group(4), "TODO", leftQuote+m.group(5)+rightQuote)
				} else {
					LogRecord( systName, m.group(1), m.group(3), "TODO", leftQuote+m.group(5)+rightQuote)
				}
			}
		}
	}

	val logData = sc.textFile(inputName)

	def accessLogs = logData.map( parseLogLine(PATTERN) ).toDF()

	accessLogs.show()
	accessLogs.write.format("com.databricks.spark.csv").option("delimiter",";").option("quote", "\u0000").save(outputName)


	val path = outputName + "/"
	val conf = new Configuration()
	val fs = FileSystem.get(conf)
	val p = new Path(path)
	val ls = fs.listStatus(p)
	 
	ls.foreach( x => {
		val f = x.getPath.toString

		if(f == "file:"+outputName+"/.DS_Store" || f == "file:"+outputName+"/_SUCCESS") {
		} else {
			val formattedLog = spark.read.option("delimiter",";").csv(f).toDF

			val newFormattedLog = formattedLog.withColumn("_c3",callUDF("findNull",struct(formattedLog.columns.map(formattedLog(_)) : _*) ))
			newFormattedLog.show
			// newFormattedLog.write.format("com.databricks.spark.csv").option("delimiter",";").save(f+"Final.csv")

			if(dateFormatName != "NO"){
				println("dateFormatName: " + dateFormatName)
				val modif = newFormattedLog.withColumn("Date",unix_timestamp(newFormattedLog.col("_c1"), dateFormatName)).drop("_c1")
				
				var strCustDate = "YYYY-MM-dd hh:mm:ss"
				if(systName == "ANDROID"){
					strCustDate = "20YY-MM-dd HH:mm:ss"
				}

				val tsar = modif.withColumn("Date", to_utc_timestamp(from_unixtime($"Date"),strCustDate)).alias("timestamp")
				tsar.show
				
				val new_df = tsar.select("_c0", "Date", "_c2", "_c3", "_c4")
				new_df.show

				val tempDir = f + "_tmp"
				new_df.write.format("com.databricks.spark.csv").option("delimiter",";").option("timestampFormat", "YYYY-MM-dd HH:mm:ss").save(f+"this.csv")
			} else {
				println("NOTHING TO FORMAT")
				newFormattedLog.write.format("com.databricks.spark.csv").option("delimiter",";").option("timestampFormat", "YYYY-MM-dd HH:mm:ss").save(f+"this.csv")
			}
		}
	} )

    valeur = valeur - 1
}






















