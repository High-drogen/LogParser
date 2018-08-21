

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import org.apache.spark.sql.Row

// val formattedLog = spark.read.option("delimiter", ";").csv("/Users/vaati/Desktop/loghub/Hadoop/Hadoop.csv/part-00001-a7a384e0-76f9-4faa-91e8-0ea9545ce58f-c000.csv").toDF
// formattedLog.show

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

val path = "/Users/vaati/Desktop/LogHub Datasets/Zookeeper/Zookeeper.csv/"
val conf = new Configuration()
val fs = FileSystem.get(conf)
val p = new Path(path)
val ls = fs.listStatus(p)
 
ls.foreach( x => {
	val f = x.getPath.toString
	println("SADJHSALFHLKDJFLK : " +f)

	if(f == "file:/Users/vaati/Desktop/LogHub Datasets/Zookeeper/Zookeeper.csv/.DS_Store" || f == "file:/Users/vaati/Desktop/LogHub Datasets/Zookeeper/Zookeeper.csv/_SUCCESS") {
	} else {
		val formattedLog = spark.read.option("delimiter",";").csv(f).toDF

		// spark.sqlContext.udf.register("findNull", findNull _)
		val newFormattedLog = formattedLog.withColumn("_c3",callUDF("findNull",struct(formattedLog.columns.map(formattedLog(_)) : _*) ))
		// newFormattedLog.show
		newFormattedLog.write.format("com.databricks.spark.csv").option("delimiter",";").save(f+"Final.csv")

	}
} )

/*
|  30  |  50  |   40   |  10   | 20   | 67   |  61   |  65    |   10   | 60    |   30
|----------------------------------------------------------------------------------------
|      |      |w       |       |      |      |       |f       |        |       | NOTICE
|      |      |wa      |       |      |      |s      |fa      |        |       |
|      |e     |war     |t      |d     |f     |se     |fai     |        |       |
|i     |er    |warn    |tr     |de    |fa    |sev    |fail    |        |       |
|in    |err   |warni   |tra    |deb   |fat   |seve   |failu   |        |       |
|inf   |erro  |warnin  |trac   |debu  |fata  |sever  |failur  |        |       |
|info  |error |warning |trace  |debug |fatal |severe |failure |        |       |
|Info  |Error |Warning |Trace  |Debug |Fatal |Severe |Failure |        |       |
|INfo  |ERror |WArning |TRace  |DEbug |FAtal |SEvere |FAilure |        |       |
|INFo  |ERRor |WARning |TRAce  |DEBug |FATal |SEVere |FAIlure |        |       |
|INFO  |ERROr |WARNing |TRACE  |DEBUg |FATAl |SEVEre |FAILure |        |       |
|INF   |ERROR |WARNIng |TRAC   |DEBUG |FATAL |SEVERe |FAILUre |        |       |
|IN    |ERRO  |WARNINg |TRA    |DEBU  |FATA  |SEVERE |FAILURe |        |       |            
|I     |ERR   |WARNING |TR     |DEB   |FAT   |SEVER  |FAILURE |        |       |           
|      |ER    |WARNIN  |T      |DE    |FA    |SEVE   |FAILUR  |        |       |
|      |E     |WARNI   |       |D     |F     |SEV    |FAILU   |        |       |
|      |	  |WARN    |       |	  |      |SE     |FAIL    |        |       |
|      |	  |WAR     |       |      |      |S      |FAI     |        |       |
|      |	  |WA      |       |      |      |       |FA      |        |       |
|      |	  |W       |	   |      |      |       |F       |V       |C      |
*/
























