package net.nano.dynamicetl

import org.apache.commons.cli.CommandLine
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.spark.sql.sparkDatasetFunctions

import java.util.concurrent.{Executors, TimeUnit}

class DynamicETLEngine(val spark: SparkSession) {
  val sc = spark.sparkContext

  implicit class StringPath(val path: String) {
    val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    def fileExist: Boolean = {
      fs.exists(new Path(path))
    }
  }

  def readPKOffset(path: String): Integer = {
    if (path.fileExist) {
      spark.read.parquet(path).select("lastWindow").head.getAs[Integer]("lastWindow")
    } else {
      Integer.valueOf(0)
    }
  }

  def savePKOffset(pathOffset: String, pk: Integer) {

    val id = scala.Array(Row(pk))
    val rdd = sc.parallelize(id)

    val schema = StructType(Array(StructField("lastwindow", IntegerType, true)))

    spark.createDataFrame(rdd, schema)
      .write.mode("overwrite")
      .format("parquet")
      .save(pathOffset)
  }


  def getdata(query: String, db: String) = {
    spark.read.format("jdbc")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", db)
      .option("query", query)
      .option("user", "NANO_Brain_RND_Dev_P")
      .option("password", "P@ssw0rd")
      .load()
  }

  def saveTOElastic(df: DataFrame, index: String, id: String) {
    df.saveToEs(index, Map("es.mapping.id" -> id))
  }


  def pipeline(queryName: String, query: String, db: String, trackingColumnName: String) = {
    var data: DataFrame = null

    val lastpkOffset = readPKOffset(s"/offsets/$queryName")
    data = getdata(query, db).filter(col(trackingColumnName) > lastpkOffset)

    if (data.count > 0) {
      val newOffset = data
        .orderBy(desc(trackingColumnName))
        .select(trackingColumnName).head.getAs[Integer](0)
      savePKOffset(s"/offsets/$queryName", newOffset)
      saveTOElastic(data,"erxrquests/_doc","RXId")
    }
  }

  def process(db: String): Unit ={

  }
}

object ETLEngine extends App {
  val spark = SparkSession.builder().getOrCreate()

  val executor = Executors.newSingleThreadScheduledExecutor()
  val cmd = parseArgs(args)

  val db = "jdbc:sqlserver://192.168.5.222;database=Nano_eClaimsProviders"
  val etl = new DynamicETLEngine(spark)

  val runnable = new Runnable {
    override def run(): Unit = etl.process(db)
  }
  executor.scheduleAtFixedRate(
    runnable,
    0,
    1,
    TimeUnit.DAYS
  )

  while(true)
    Thread.sleep(60000)

  def parseArgs(args: Array[String]): CommandLine = {
    import org.apache.commons.cli._
    val parser = new BasicParser
    parser.parse(options(), args)
  }

  def options() = {
    import org.apache.commons.cli._
    val options = new Options
    val configOption = new Option("c", "configuration-file",
      true, "Configuration File")
    configOption.setRequired(false)
    options.addOption(configOption)
    options
  }
}



