
import org.joda.time.{DateTimeZone}
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.Row
// import spark.implicits._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
 
val path = "/Users/vaati/Desktop/LogHub Datasets/BGL/BGL.csv/" // HERE
val conf = new Configuration()
val fs = FileSystem.get(conf)
val p = new Path(path)
val ls = fs.listStatus(p)
 
ls.foreach( x => {
val f = x.getPath.toString

if(f == "file:/Users/vaati/Desktop/LogHub Datasets/BGL/BGL.csv/.DS_Store"  || f == "file:/Users/vaati/Desktop/LogHub Datasets/BGL/BGL.csv/_SUCCESS") { // HERE
} else {
	val content = spark.read.option("delimiter",";").csv(f)
	content.show()

	val dataset = spark.read.option("delimiter", ";").csv(f).toDF
	val modif = dataset.withColumn("Date",unix_timestamp(dataset.col("_c1"), "YYYY-MM-dd-HH.mm.ss")).drop("_c1")						// HERE
	val tsar = modif.withColumn("Date", to_utc_timestamp(from_unixtime($"Date"),"YYYY-MM-dd hh:mm:ss")).alias("timestamp")				// HERE
	tsar.show
	val new_df = tsar.select("_c0", "Date", "_c2", "_c3", "_c4")
	new_df.show

	val tempDir = f + "_tmp"
	// new_df.write.format("com.databricks.spark.csv").option("delimiter",";").option("header",true).option("inferSchema","true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").save("/Users/vaati/Desktop/LogHub Datasets/Apache/ApacheFormatted.csv")
	new_df.write.format("com.databricks.spark.csv").option("delimiter",";").option("timestampFormat", "YYYY-MM-dd HH:mm:ss").save(f+"this.csv")
}
} )


























