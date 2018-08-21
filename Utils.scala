
import org.joda.time.{DateTimeZone}
import org.joda.time.format.DateTimeFormat

import org.apache.spark.sql.Row

// import spark.implicits._

// case class MyTimeStamp(timestamp: String)
// case class LogRecord(_c0: String, _c1: String, _c2: String, _c3: String, _c4: String)

// timestamp: String
// def formatTimestamp(timestamp: String) = {

// 	val df = sc.parallelize(Seq(timestamp)).toDF("dateCol")
// 	val modif = df.withColumn("Date",unix_timestamp(df.col("dateCol"), "EEE MMM dd HH:mm:ss yyyy"))
// 	val timeInMillisec = modif.select("Date")
// 	timeInMillisec.select(to_utc_timestamp(from_unixtime($"Date"),"yyyy-MM-dd hh:mm:ss").alias("timestamp"))
// }

// val strTimestamp = formatTimestamp("Sun Dec 04 04:47:44 2005")
// strTimestamp.show

// val name = strTimestamp.limit(1).select("timestamp").as[String].first()
// println(name)
/*--------------------------------------------------------------------------------*/

// def formatTimestamp(timestamp: String) = {

// 	val df = sc.parallelize(Seq(timestamp)).toDF("dateCol")
// 	val modif = df.withColumn("Date",unix_timestamp(df.col("dateCol"), "yyyy-MM-dd-HH.mm.ss"))
// 	val timeInMillisec = modif.select("Date")
// 	timeInMillisec.select(to_utc_timestamp(from_unixtime($"Date"),"yyyy-MM-dd hh:mm:ss").alias("timestamp"))
// }

// val strTimestamp = formatTimestamp("2005-06-03-15.42.51")
// strTimestamp.show

// val name = strTimestamp.limit(1).select("timestamp").as[String].first()

/*--------------------------------------------------------------------------------*/

// val dataset = spark.read.option("delimiter", ";").csv("/Users/vaati/Desktop/LogHub Datasets/Apache/Apache.csv/part-00000-8a749eb0-46d2-471d-b720-02fcd3ebb016-c000.csv").toDF
// // val modif = dataset.withColumn("Date",unix_timestamp(dataset.col("_c1"), "EEE MMM dd HH:mm:ss yyyy").cast("timestamp")).drop("_c1")

// /*----*/
// val modif = dataset.withColumn("Date",unix_timestamp(dataset.col("_c1"), "EEE MMM dd HH:mm:ss yyyy")).drop("_c1")
// val tsar = modif.withColumn("Date", to_utc_timestamp(from_unixtime($"Date"),"yyyy-MM-dd hh:mm:ss")).alias("timestamp")
// tsar.show
// /*----*/

// // modif.show
// val new_df = tsar.select("_c0", "Date", "_c2", "_c3", "_c4")
// new_df.show

/*--------------------------------------------------------------------------------*/

// import org.apache.hadoop.fs.Path
// import org.apache.hadoop.conf.Configuration
// import org.apache.hadoop.fs.FileSystem
 
// val path = "/Users/vaati/Desktop/LogHub Datasets/Apache/Apache.csv/"
// val conf = new Configuration()
// val fs = FileSystem.get(conf)
// val p = new Path(path)
// val ls = fs.listStatus(p)
 
// ls.foreach( x => {
// val f = x.getPath.toString
// println("SADJHSALFHLKDJFLK : " +f)

// if(f == "file:/Users/vaati/Desktop/LogHub Datasets/Apache/Apache.csv/.DS_Store") {
// } else {
// 	val content = spark.read.option("delimiter",";").csv(f)
// 	content.show()

// 	val dataset = spark.read.option("delimiter", ";").csv(f).toDF
// 	val modif = dataset.withColumn("Date",unix_timestamp(dataset.col("_c1"), "EEE MMM dd HH:mm:ss yyyy")).drop("_c1")
// 	val tsar = modif.withColumn("Date", to_utc_timestamp(from_unixtime($"Date"),"yyyy-MM-dd hh:mm:ss")).alias("timestamp")
// 	tsar.show
// 	val new_df = tsar.select("_c0", "Date", "_c2", "_c3", "_c4")
// 	new_df.show

// 	val tempDir = f + "_tmp"
// 	// new_df.write.format("com.databricks.spark.csv").option("delimiter",";").option("header",true).option("inferSchema","true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").save("/Users/vaati/Desktop/LogHub Datasets/Apache/ApacheFormatted.csv")
// 	new_df.write.format("com.databricks.spark.csv").option("delimiter",";").option("header",true).option("inferSchema","true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").save(f+"this.csv")
// }
// } )


/*--------------------------------------------------------------------------------*/

// import org.apache.hadoop.fs.Path
// import org.apache.hadoop.conf.Configuration
// import org.apache.hadoop.fs.FileSystem
 
// val path = "/Users/vaati/Desktop/LogHub Datasets/BGL/BGL.csv/"
// val conf = new Configuration()
// val fs = FileSystem.get(conf)
// val p = new Path(path)
// val ls = fs.listStatus(p)
 
// ls.foreach( x => {
// val f = x.getPath.toString
// println("SADJHSALFHLKDJFLK : " +f)

// if(f == "file:/Users/vaati/Desktop/LogHub Datasets/BGL/BGL.csv/.DS_Store") {
// } else {
// 	val content = spark.read.option("delimiter",";").csv(f)
// 	content.show()

// 	val dataset = spark.read.option("delimiter", ";").csv(f).toDF
// 	val modif = dataset.withColumn("Date",unix_timestamp(dataset.col("_c1"), "yyyy-MM-dd-HH.mm.ss")).drop("_c1")
// 	val tsar = modif.withColumn("Date", to_utc_timestamp(from_unixtime($"Date"),"yyyy-MM-dd hh:mm:ss")).alias("timestamp")
// 	tsar.show
// 	val new_df = tsar.select("_c0", "Date", "_c2", "_c3", "_c4")
// 	new_df.show

// 	val tempDir = f + "_tmp"
// 	// new_df.write.format("com.databricks.spark.csv").option("delimiter",";").option("header",true).option("inferSchema","true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").save("/Users/vaati/Desktop/LogHub Datasets/Apache/ApacheFormatted.csv")
// 	new_df.write.format("com.databricks.spark.csv").option("delimiter",";").option("header",true).option("inferSchema","true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").save(f+"this.csv")
// }
// } )

/*--------------------------------------------------------------------------------*/

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
 
val path = "/Users/vaati/Desktop/LogHub Datasets/BGL/BGL.csv/"
val conf = new Configuration()
val fs = FileSystem.get(conf)
val p = new Path(path)
val ls = fs.listStatus(p)
 
ls.foreach( x => {
val f = x.getPath.toString
// println("SADJHSALFHLKDJFLK : " +f)

if(f == "file:/Users/vaati/Desktop/LogHub Datasets/BGL/BGL.csv/.DS_Store"  || f == "file:/Users/vaati/Desktop/LogHub Datasets/BGL/BGL.csv/_SUCCESS") {
} else {
	val content = spark.read.option("delimiter",";").csv(f)
	content.show()

	val dataset = spark.read.option("delimiter", ";").csv(f).toDF
	val modif = dataset.withColumn("Date",unix_timestamp(dataset.col("_c1"), "YYYY-MM-dd-HH.mm.ss")).drop("_c1")
	val tsar = modif.withColumn("Date", to_utc_timestamp(from_unixtime($"Date"),"YYYY-MM-dd hh:mm:ss")).alias("timestamp")
	tsar.show
	val new_df = tsar.select("_c0", "Date", "_c2", "_c3", "_c4")
	new_df.show

	val tempDir = f + "_tmp"
	// new_df.write.format("com.databricks.spark.csv").option("delimiter",";").option("header",true).option("inferSchema","true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").save("/Users/vaati/Desktop/LogHub Datasets/Apache/ApacheFormatted.csv")
	new_df.write.format("com.databricks.spark.csv").option("delimiter",";").option("timestampFormat", "YYYY-MM-dd HH:mm:ss").save(f+"this.csv")
}
} )


























