import scala.util.matching.Regex
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.Row
import spark.implicits._
import org.apache.spark.rdd.RDD

// -------------------------------------------------------------------------------------------------------------------------------

// case class parsingInfo(system: String, pattern: String, input: String, output: String)

// def foo(r: Row) = {

// 	parsingInfo( r.getAs[String]("system"), r.getAs[String]("pattern"), r.getAs[String]("input"), r.getAs[String]("output"))
// }
val leftQuote = "\u201C"
val rightQuote = "\u201D"

val sqlContext = new SQLContext(sc)
val df = sqlContext.read.option("multiline", true).json("/Users/vaati/Desktop/LogParser/LogParser/info.json").toDF
val df2 = df.withColumn("info", explode($"info")).withColumn("system", $"info"(0)).withColumn("pattern", $"info"(1)).withColumn("input", $"info"(2)).withColumn("output", $"info"(3)).drop("info").withColumn("id",monotonicallyIncreasingId)
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

	println("Name : " + systName)
	println("Name : " + patternName)
	println("Name : " + inputName)
	println("Name : " + outputName)

	val PATTERN = patternName.r

	/*---------------------------------------------------------------------------------------------------------------------------*/

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

	/*---------------------------------------------------------------------------------------------------------------------------*/


    valeur = valeur - 1
}



// val rddRows: RDD[Row] =
//       df2.rdd.map(row => {
      
//         val lstRow = row.toSeq.toList
        
//         var lstRowNew = lstRow
//         // do stuff on the new lstRow here 
//         println(lstRowNew(1))

//         Row.fromSeq(lstRowNew)
//       })

// rddRows.getClass

// val dfOut = spark.createDataFrame(rddRows, df2.schema)



// df2.map( r => { 
// 	foo
// 	println("HAHAHA")
// })

 // df2.dtypes.foreach {  f =>
 //      val fName = f._1
 //      val fType = f._2
 //      if (fType  == "StringType") { println(s"STRING_TYPE") }
 //      if (fType  == "MapType") { println(s"MAP_TYPE") }
 //      //else {println("....")}
 //      println("Name %s Type:%s - all:%s".format(fName , fType, f))

 //    }

 // df2.foreach { row => {
 // 						val system = println(row(0))
 // 						val pattern = println(row(1))
 // 						val input = println(row(2).toString)
 // 						val output = println(row(3))
 // 					}
           // row.toSeq.foreach{ 
           // 		col => {
           // 			println(col)
           // 		}
           // }
    // }


// df.registerTempTable("tempSmthg")
// val smthg = sqlContext.sql("SELECT * FROM tempSmthg")
// smthg.show
// smthg.printSchema

// val child_pattern = df.select("info")
// // child_pattern.registerTempTable("tempcust")
// val pattern = sqlContext.sql("SELECT info.system FROM tempcust").first()
// println("PATTERN: " + pattern)
// pattern.getClass
// println(pattern.getAs[Seq[String]]("system")(0))


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
			LogRecord( "Android", m.group(1), m.group(2), leftQuote+m.group(3)+rightQuote )
		}
	}
}

val logData = sc.textFile("/Users/vaati/Desktop/loghub/Andriod/Andriod_2k.log")

def accessLogs = logData.map( parseLogLine(PATTERN) ).toDF()

accessLogs.show()
*/
//accessLogs.write.format("com.databricks.spark.csv").option("delimiter","|").save("/Users/vaati/Desktop/loghub/Andriod/Android.csv")
