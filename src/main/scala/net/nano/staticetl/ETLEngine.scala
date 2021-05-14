package net.nano.staticetl

import org.apache.commons.cli.{BasicParser, CommandLine}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, collect_set, desc, struct}
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.Timestamp
import java.util.concurrent.{Executors, TimeUnit}

class ETLEngine(val spark: SparkSession) {
  val sc = spark.sparkContext

  implicit class StringPath(val path: String) {
    val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    def fileExist: Boolean = {
      fs.exists(new Path(path))
    }
  }

  def readOffset(path: String): Timestamp = {
    if (path.fileExist) {
      spark.read.parquet(path).select("lastWindow").head.getAs[Timestamp]("lastWindow")
    } else {
      // Timestamp.from(Instant.MIN)
      Timestamp.valueOf("1900-01-01 23:00:01")
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

  def saveOffset(pathOffset: String, timestamp: Timestamp) {

    val date = scala.Array(Row(timestamp))
    val rdd = sc.parallelize(date)

    val schema = StructType(Array(StructField("lastwindow", TimestampType, true)))

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


  def writeDataHdfs(data: DataFrame, pathData: String) = {
    data.write
      .mode("overwrite")
      .format("parquet")
      .save(pathData)

  }

  def pipelinePerTypeTrack(queryName: String, query: String, db: String, trackingColumnName: String, typeTrack: String) = {
    var data: DataFrame = null

    if (typeTrack == "timestamp") {
      val lastOffset = readOffset(s"/offsets/$queryName")
      data = getdata(query, db).filter(col(trackingColumnName) > lastOffset)

    }
    else {
      val lastpkOffset = readPKOffset(s"/offsets/$queryName")
      data = getdata(query, db).filter(col(trackingColumnName) > lastpkOffset)

    }

    if (data.count > 0) {
      if (typeTrack == "timestamp") {
        val newOffset = data.orderBy(desc(trackingColumnName)).select(trackingColumnName).head.getAs[Timestamp](0)
        saveOffset(s"/offsets/$queryName", newOffset)
      } else {
        val newOffset = data.orderBy(desc(trackingColumnName)).select(trackingColumnName).head.getAs[Integer](0)
        savePKOffset(s"/offsets/$queryName", newOffset)
      }
      writeDataHdfs(data, s"/profiles/$queryName")

    }


  }

  def pipelineProviderSpeciality(queryName: String, query: String, db: String, trackingColumnName: String, typeTrack: String) =
  {
    var data: DataFrame = null

    val lastOffset = readOffset(s"/masters/offsets/$queryName")
    data = getdata(query, db) //.filter(col(trackingColumnName) > lastOffset)

    if (data.count > 0) {
      val newOffset = data.orderBy(desc(trackingColumnName)).select(trackingColumnName).head.getAs[Timestamp](0)
      saveOffset(s"/masters/offsets/$queryName", newOffset)


      writeDataHdfs(
        data.groupBy("Pkey", "ProviderName", "LicenseID", "RegulatoryKey", "ParentGroupKey", "Citykey", "Phoneno", "Email", "Address", "Website", "CityAreaKey", "Createdat", "Updateat")
          .agg(collect_set(struct(col("SpecialityName"), col("SpecialityCategory"))).as("Specialities"))
        , s"/masters/profiles/$queryName")
    }


  }

  def pipelineClinicianDetails(queryName: String, query: String, db: String, trackingColumnName: String, typeTrack: String) =
  {
    var data: DataFrame = null

    val lastOffset = readOffset(s"/masters/offsets/$queryName")
    data = getdata(query, db)

    if (data.count > 0) {
      val newOffset = data.orderBy(desc(trackingColumnName)).select(trackingColumnName).head.getAs[Timestamp](0)
      saveOffset(s"/masters/offsets/$queryName", newOffset)

      writeDataHdfs(
        data.groupBy("Pkey", "ClinicianName", "LicenseID", "Createdat", "Updatedat", "SpecialityKey",
          "SpecialityName", "Gender", "CategoryKey", "CategoryName", "Departmentkey", "DepartmentName", "LanguageName","LanguageCode")
          .agg(collect_set(struct(col("ProviderName"), col("providerEmail"),
            col("providerAddres"))))
        , s"/masters/profiles/$queryName")
      }

  }

  def saveStaticData(db: String) {

    val clinician = s""" SELECT * FROM [NANO_Brain_RND_Dev].[dbo].[ClinicianMaster]"""
    pipelinePerTypeTrack("clinician", clinician, db, "LastEditDateTime", "timestamp")

    val facility = s"""SELECT * FROM [NANO_Brain_RND_Dev].[dbo].[FacilityMaster]"""
    pipelinePerTypeTrack("facility", facility, db, "LastEditDateTime", "timestamp")

    val drugs = s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].IDDK_Drugs"""
    pipelinePerTypeTrack("drugs", drugs, db, "LastEditedDatetime", "timestamp")

    val ICD10CMCodes = s"""SELECT * FROM [NANO_Brain_RND_Dev].dbo.ICD10CMCodes"""
    pipelinePerTypeTrack("ICD10CMCodes", ICD10CMCodes, db, "Pkey", "pk")

    val ICDCodeMajorSubCategory = s"""SELECT * FROM NANO_Brain_RND_Dev.dbo.ICDCodeMajorSubCategory"""
    pipelinePerTypeTrack("ICDCodeMajorSubCategory", ICDCodeMajorSubCategory, db, "Pkey", "pk")

    val denialReasons = s"""SELECT * FROM [Nano_eClaimsProviders].[dbo].[DenialReasons]"""
    pipelinePerTypeTrack("denialReasons", denialReasons, db, "DenialReasonId", "pk")

    val payers = s"""SELECT * FROM [Nano_eClaimsProviders].[dbo].[Payers]"""
    pipelinePerTypeTrack("payers", payers, db, "PayerId", "pk")

  }

  def saveMastersData(db: String): Unit = {
    val NANO_Master_Country =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_Country]"""
    pipelinePerTypeTrack("NANO_Master_Country", NANO_Master_Country, db, "UpdatedAt", "timestamp")

    val NANO_Master_Province =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_Province]"""
    pipelinePerTypeTrack("NANO_Master_Province", NANO_Master_Province, db, "Updatedat", "timestamp")

    val NANO_Master_City =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_City]"""
    pipelinePerTypeTrack("NANO_Master_City", NANO_Master_City, db, "Updatedat", "timestamp")

    val NANO_Master_Area =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_Area]"""
    pipelinePerTypeTrack("NANO_Master_Area", NANO_Master_Area, db, "Updatedat", "timestamp")

    val NANO_Master_HealthAuthority =
      s""" SELECT * FROM  [Nano_eClaimsProviders].[dbo].[NANO_Master_HealthAuthority]"""
    pipelinePerTypeTrack("NANO_Master_HealthAuthority", NANO_Master_HealthAuthority, db, "Updatedat", "timestamp")

    val NANO_Master_Clinician =
      s"""
       c.[Pkey]
      ,[ClinicianName]
      ,c.[LicenseID]
      ,c.[Createdat]
      ,c.[Updatedat]
      ,[SpecialityKey]
      ,sp.SpecialityName
      ,[Gender]
      ,[CategoryKey]
      ,cat.[CategoryName]
      ,[Departmentkey],
      [DepartmentName],
      LanguageName,
      LanguageCode
      ,pro.ProviderName,
      pro.Email as providerEmail
      ,pro.LicenseID as providerLicenseID
      ,pro.Address as providerAddres

      FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_Clinician] c
      join [Nano_eClaimsProviders].[dbo].[NANO_Mapping_Clinician_Language] clan on c.Pkey = clan.[ClinicianKey]
      join [Nano_eClaimsProviders].[dbo].[NANO_Master_Language] lan on clan.[LanguageKey] = lan.Pkey
      join [Nano_eClaimsProviders].[dbo].[NANO_Master_Clinician_Speciality] sp on sp.Pkey = c.[SpecialityKey]
      join [Nano_eClaimsProviders].[dbo].[NANO_Master_Clinician_Category] cat on cat.Pkey = c.[CategoryKey]
      join [Nano_eClaimsProviders].[dbo].NANO_Master_Clinician_Department dep on dep.Pkey = c.[Departmentkey]
      join [Nano_eClaimsProviders].[dbo].NANO_Mapping_Clinician_Provider clpro on clpro.ClinicianKey = c.Pkey
      join [Nano_eClaimsProviders].[dbo].NANO_Master_Provider pro on clpro.ProviderKey = pro.Pkey"""
    pipelineClinicianDetails("NANO_Master_Clinician", NANO_Master_Clinician, db, "Updatedat", "timestamp")

    val NANO_Master_Payer =
      s""" SELECT  *
         |  FROM
         |  [Nano_eClaimsProviders].[dbo].[NANO_Master_Payer] p
         |  join [Nano_eClaimsProviders].[dbo].[NANO_Master_Payer_Type] pt on p.PayerTypeKey = pt.Pkey
         |  join [Nano_eClaimsProviders].[dbo].[NANO_Master_PayerBenefit] pb on pb.PayerKey = p.Pkey
         |  """.stripMargin
    pipelinePerTypeTrack("NANO_Master_Payer", NANO_Master_Payer, db, "Updatedat", "timestamp")


    val NANO_Master_PayerMember =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_PayerMember]"""
    pipelinePerTypeTrack("NANO_Master_PayerMember", NANO_Master_PayerMember, db, "Updatedat", "timestamp")

    val NANO_PayerDetailsPolicy =
      s""" SELECT  *
         |  FROM [Nano_eClaimsProviders].[dbo].[NANO_PayerPolicy] pp
         |  join [Nano_eClaimsProviders].[dbo].[NANO_Master_Currency] c on c.Pkey = pp.CurrencyKey
         |  join [Nano_eClaimsProviders].[dbo].[NANO_PayerPolicyBenefit] ppb on ppb.PolicyKey = pp.Pkey
         |  join [Nano_eClaimsProviders].[dbo].[NANO_Master_PayerBenefit] pb on pb.Pkey = ppb.BenefitKey""".stripMargin
    pipelinePerTypeTrack("NANO_PayerDetailsPolicy", NANO_PayerDetailsPolicy, db, "Updatedat", "timestamp")

    val NANO_Master_Provider =
      s""" SELECT
      pr.[Pkey]
      ,[ProviderName]
      ,[LicenseID]
      ,[RegulatoryKey]
      ,[ParentGroupKey]
      ,[Citykey]
      ,[Phoneno]
      ,[Email]
      ,[Website]
      ,[Address]
      ,[CityAreaKey]
	    ,[SpecialityName]
      ,[SpecialityCategory]
      ,pr.[Createdat]
      ,pr.[Updateat],
	  pg.GroupName,
	  pg.Email,
	  pg.MainOfficeCountryKey,
	  pg.Website
      FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_Provider] pr
      join [Nano_eClaimsProviders].[dbo].[NANO_Mapping_Provider_Speciality] msp on pr.Pkey = msp.ProviderKey
      join [Nano_eClaimsProviders].[dbo].[NANO_Master_Provider_Speciality] sp on msp.SpecialityKey = sp.Pkey
	  join [Nano_eClaimsProviders].[dbo].[NANO_Mapping_Provider_ProviderGroup] ppg on ppg.ProviderMasterKey = pr.Pkey
	  join [Nano_eClaimsProviders].[dbo].[NANO_Master_ProviderGroup] pg on pg.Pkey = ppg.GroupMasterKey
"""
    pipelineProviderSpeciality("NANO_Master_Provider", NANO_Master_Provider, db, "Updatedat", "timestamp")


  }
}


object ETLEngine extends App {
  val spark = SparkSession.builder().getOrCreate()

  val executor = Executors.newSingleThreadScheduledExecutor()
  val cmd = parseArgs(args)

  val db = "jdbc:sqlserver://192.168.5.222;database=Nano_eClaimsProviders"
  val etl = new ETLEngine(spark)

  val runnable = new Runnable {
    override def run(): Unit = etl.saveStaticData(db)
  }
  executor.scheduleAtFixedRate(
    runnable,
    0,
    1,
    TimeUnit.DAYS
  )

  while (true)
    Thread.sleep(60000)

  def parseArgs(args: Array[String]): CommandLine = {
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