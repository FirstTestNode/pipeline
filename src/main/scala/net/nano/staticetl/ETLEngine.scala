package net.nano.staticetl

import org.apache.commons.cli.{BasicParser, CommandLine}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, collect_set, date_format, desc, first, struct, when}
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.spark.sql.sparkDatasetFunctions

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
    }
      writeDataHdfs(data, s"/profiles/$queryName")




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

  def pipelinePayerProfile(queryName: String, query: String, db: String, trackingColumnName: String, typeTrack: String) = {
    var data: DataFrame = null

    val lastOffset = readOffset(s"/masters/offsets/$queryName")
    data = getdata(query, db) //.filter(col(trackingColumnName) > lastOffset)

    if (data.count > 0) {
      val newOffset = data.orderBy(desc(trackingColumnName)).select(trackingColumnName).head.getAs[Timestamp](0)
      saveOffset(s"/masters/offsets/$queryName", newOffset)

    }
    writeDataHdfs(
      data.groupBy("Pkey", "PayerName", "CountryKey", "PayerTypeKey", "Email", "Address", "Website", "CreatedAt", "UpdatedAt", "PayerType")
        .agg(collect_set(struct(col("PkeyBenefit"), col("BenefitCode"))).as("BenefitName"))
      , s"/masters/profiles/$queryName")



  }

  def pipelineProviderSpeciality(queryName: String, query: String, db: String, trackingColumnName: String, typeTrack: String) =
  {
    var data: DataFrame = null

    val lastOffset = readOffset(s"/masters/offsets/$queryName")
    data = getdata(query, db) //.filter(col(trackingColumnName) > lastOffset)

    if (data.count > 0) {
      val newOffset = data.orderBy(desc(trackingColumnName)).select(trackingColumnName).head.getAs[Timestamp](0)
      saveOffset(s"/masters/offsets/$queryName", newOffset)
    }

    writeDataHdfs(
      data.groupBy("Pkey", "ProviderName", "LicenseID", "RegulatoryKey", "ParentGroupKey", "Citykey", "Phoneno", "Email", "Address", "Website", "CityAreaKey", "Createdat", "Updateat")
        .agg(collect_set(struct(col("SpecialityName"), col("SpecialityCategory"))).as("Specialities"))
      , s"/masters/profiles/$queryName")



  }

  def pipelineClinicianDetails(queryName: String, query: String, db: String, trackingColumnName: String, typeTrack: String) =
  {
    var data: DataFrame = null

    val lastOffset = readOffset(s"/masters/offsets/$queryName")
    data = getdata(query, db)

    if (data.count > 0) {
      val newOffset = data.orderBy(desc(trackingColumnName)).select(trackingColumnName).head.getAs[Timestamp](0)
      saveOffset(s"/masters/offsets/$queryName", newOffset)
    }
    writeDataHdfs(
      data.groupBy("Pkey", "ClinicianName", "LicenseID", "Createdat", "Updatedat", "SpecialityKey",
        "SpecialityName", "Gender", "CategoryKey", "CategoryName", "Departmentkey", "DepartmentName")
        .agg(collect_set(struct(col("ProviderName"),col("providerEmail"),col("providerAddres"))).as("ProviderDetails")
          ,collect_set(struct( col("LanguageName"),col("LanguageCode"))).as("LanguageDetails"))
      , s"/masters/profiles/$queryName")


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

    val NANO_Mapping_Area_RelatedArea =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_Mapping_Area_RelatedArea]"""
    pipelinePerTypeTrack("NANO_Mapping_Area_RelatedArea", NANO_Mapping_Area_RelatedArea, db, "Updatedat", "timestamp")

    val NANO_Master_HealthAuthority =
      s""" SELECT * FROM  [Nano_eClaimsProviders].[dbo].[NANO_Master_HealthAuthority]"""
    pipelinePerTypeTrack("NANO_Master_HealthAuthority", NANO_Master_HealthAuthority, db, "Updatedat", "timestamp")

    val NANO_Master_Clinician_AllDetails =
      s"""
      SELECT
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
    pipelineClinicianDetails("NANO_Master_Clinician_AllDetails", NANO_Master_Clinician_AllDetails, db, "Updatedat", "timestamp")


    val NANO_Master_Clinician =
      s""" SELECT * FROM  [Nano_eClaimsProviders].[dbo].[NANO_Master_Clinician]"""
    pipelinePerTypeTrack("NANO_Master_Clinician", NANO_Master_Clinician, db, "Updatedat", "timestamp")

    val NANO_Master_Language =
      s""" SELECT * FROM  [Nano_eClaimsProviders].[dbo].[NANO_Master_Language]"""
    pipelinePerTypeTrack("NANO_Master_Language", NANO_Master_Language, db, "Updatedat", "timestamp")
    val NANO_Master_Clinician_Speciality =
      s""" SELECT * FROM  [Nano_eClaimsProviders].[dbo].[NANO_Master_Clinician_Speciality]"""
    pipelinePerTypeTrack("NANO_Master_Clinician_Speciality", NANO_Master_Clinician_Speciality, db, "Updatedat", "timestamp")
    val NANO_Master_Clinician_Category =
      s""" SELECT * FROM  [Nano_eClaimsProviders].[dbo].[NANO_Master_Clinician_Category]"""
    pipelinePerTypeTrack("NANO_Master_Clinician_Category", NANO_Master_Clinician_Category, db, "Updatedat", "timestamp")

    val NANO_Master_Clinician_Department =
      s""" SELECT * FROM  [Nano_eClaimsProviders].[dbo].[NANO_Master_Clinician_Department]"""
    pipelinePerTypeTrack("NANO_Master_Clinician_Department", NANO_Master_Clinician_Department, db, "Updatedat", "timestamp")


    val NANO_Master_Payer_AllDetails =
      s"""SELECT
      p. [Pkey]
     ,p.[PayerName]
      ,p.[CountryKey]
      ,p.[PayerTypeKey]
      ,p.[Email]
      ,p.[Address]
      ,p.[Website]
      ,p.[CreatedAt]
      ,p.[UpdatedAt]
      ,pt.[PayerType]
	  ,pb.[Pkey] as PkeyBenefit
	  ,pb.[BenefitCode]
      ,pb.[BenefitName]
      ,pb.[PayerKey]
FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_Payer] p
     join [Nano_eClaimsProviders].[dbo].[NANO_Master_Payer_Type] pt on p.PayerTypeKey = pt.Pkey
     join [Nano_eClaimsProviders].[dbo].[NANO_Master_PayerBenefit] pb on pb.PayerKey = p.Pkey
        """.stripMargin
    pipelinePayerProfile("NANO_Master_Payer_AllDetails", NANO_Master_Payer_AllDetails, db, "Pkey", "pk")

    val NANO_Master_Payer =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_Payer]"""
    pipelinePerTypeTrack("NANO_Master_Payer", NANO_Master_Payer, db, "Pkey", "pk")

    val NANO_Master_Payer_Type =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_Payer_Type]"""
    pipelinePerTypeTrack("NANO_Master_Payer_Type", NANO_Master_Payer_Type, db, "Pkey", "pk")

    val NANO_Master_PayerBenefit =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_PayerBenefit]"""
    pipelinePerTypeTrack("NANO_Master_PayerBenefit", NANO_Master_PayerBenefit, db, "Pkey", "pk")




    val NANO_Master_PayerMember =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_PayerMember]"""
    pipelinePerTypeTrack("NANO_Master_PayerMember", NANO_Master_PayerMember, db, "Updatedat", "timestamp")

    val NANO_PayerDetailsPolicy =
      s""" SELECT
       pp.[Pkey]
      ,pp.[Payerkey]
      ,[StartDate]
      ,[EndDate]
      ,[CurrencyKey]
      ,[Premium]
      ,[AnnualLimit]
      ,[PolicyID]
      ,[PolicyName]
	  ,[CurrencyName]
      ,[CurrencyCode]
      ,[CurrencyNumber]
	  ,[PolicyKey]
      ,[BenefitKey]
	  ,[BenefitCode]
      ,[BenefitName]
      ,[IsCovered]
         |  FROM [Nano_eClaimsProviders].[dbo].[NANO_PayerPolicy] pp
         |  join [Nano_eClaimsProviders].[dbo].[NANO_Master_Currency] c on c.Pkey = pp.CurrencyKey
         |  join [Nano_eClaimsProviders].[dbo].[NANO_PayerPolicyBenefit] ppb on ppb.PolicyKey = pp.Pkey
         |  join [Nano_eClaimsProviders].[dbo].[NANO_Master_PayerBenefit] pb on pb.Pkey = ppb.BenefitKey""".stripMargin
    pipelinePerTypeTrack("NANO_PayerDetailsPolicy", NANO_PayerDetailsPolicy, db, "Pkey", "pk")

    val NANO_PayerPolicy =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_PayerPolicy]"""
    pipelinePerTypeTrack("NANO_PayerPolicy", NANO_PayerPolicy, db, "Pkey", "pk")


    val NANO_Master_Currency =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_Currency]"""
    pipelinePerTypeTrack("NANO_Master_Currency", NANO_Master_Currency, db, "Pkey", "pk")

    val NANO_PayerPolicyBenefit =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_PayerPolicyBenefit]"""
    pipelinePerTypeTrack("NANO_PayerPolicyBenefit", NANO_Master_PayerBenefit, db, "Pkey", "pk")

    val NANO_PayerLicense =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_PayerLicense]"""
    pipelinePerTypeTrack("NANO_PayerLicense", NANO_PayerLicense, db, "Pkey", "pk")


    val NANO_Master_Provider_AllDetails =
      s""" SELECT
      pr.[Pkey]
      ,[ProviderName]
      ,[LicenseID]
      ,[RegulatoryKey]
      ,[ParentGroupKey]
      ,[Citykey]
      ,[Phoneno]
      ,pr.[Email]
      ,pr.[Website]
      ,[Address]
      ,[CityAreaKey]
	  ,[SpecialityName]
      ,[SpecialityCategory]
      ,pr.[Createdat]
      ,pr.[Updateat],
	  pg.GroupName,
	  pg.Email as EmailGroup,
	  pg.MainOfficeCountryKey,
	  pg.Website as WebsiteGroup
      FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_Provider] pr
      join [Nano_eClaimsProviders].[dbo].[NANO_Mapping_Provider_Speciality] msp on pr.Pkey = msp.ProviderKey
      join [Nano_eClaimsProviders].[dbo].[NANO_Master_Provider_Speciality] sp on msp.SpecialityKey = sp.Pkey
	  join [Nano_eClaimsProviders].[dbo].[NANO_Mapping_Provider_ProviderGroup] ppg on ppg.ProviderMasterKey = pr.Pkey
	  join [Nano_eClaimsProviders].[dbo].[NANO_Master_ProviderGroup] pg on pg.Pkey = ppg.GroupMasterKey
"""
    pipelineProviderSpeciality("NANO_Master_Provider_AllDetails", NANO_Master_Provider_AllDetails, db, "Updatedat", "timestamp")


    val NANO_Master_Provider =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_Provider]"""
    pipelinePerTypeTrack("NANO_Master_Provider", NANO_Master_Provider, db, "Updatedat", "timestamp")

    val NANO_Master_Provider_Speciality =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_Provider_Speciality]"""
    pipelinePerTypeTrack("NANO_Master_Provider_Speciality", NANO_Master_Provider_Speciality, db, "Updatedat", "timestamp")


    val NANO_Master_ProviderGroup =
      s""" SELECT * FROM [Nano_eClaimsProviders].[dbo].[NANO_Master_ProviderGroup]"""
    pipelinePerTypeTrack("NANO_Master_ProviderGroup", NANO_Master_ProviderGroup, db, "Updatedat", "timestamp")


  }


  def pipeline_prior_request(queryName:String,query:String,db:String,trackingColumnName:String) =
  {
    var data: DataFrame = null

    val lastOffset = readOffset(s"/offsets/$queryName")

    val q = s""" ${query} where pr.CreatedOn > '${lastOffset}'"""

    data = getdata(q,db)


    if(data.count > 0){
      val newOffset = data.orderBy(desc(trackingColumnName)).select(trackingColumnName).head.getAs[Timestamp](0)
      saveOffset(s"/offsets/$queryName",newOffset)
    }

    data.withColumn("ActivityNetAmt",col("ActivityNetAmt").cast(DoubleType))
      .withColumn("PaaActivityListAmt",col("PaaActivityListAmt").cast(DoubleType))
      .withColumn("ActivityPaymentAmt",col("ActivityPaymentAmt").cast(DoubleType))
      .withColumn("ActivityPatientShareAmt",col("ActivityPatientShareAmt").cast(DoubleType))
      .withColumn("CreatedOn",date_format(col("CreatedOn"), "yyyy/MM/dd HH:mm:ss"))
      .withColumn("EndDate",date_format(col("EndDate"), "yyyy/MM/dd HH:mm:ss"))
      .groupBy(col("PRSenderID"),col("PRReceiverID"),col("CreatedOn"),col("PaSenderID"),col("PAReceiverID"),col("EndDate"),col("ClientName"),col("ClientTypeName"))
      .agg(
        collect_set(struct(col("ActivityCode"),col("ActivityNetAmt"),col("PRAActivityReference"),col("ActivityType"))).as("Activities"),
        collect_set(struct(col("PAActivityCode"),col("PaaActivityListAmt"),col("ActivityPaymentAmt"),col("PAAActivityReference"),col("ActivityPatientShareAmt"),col("DenialCode"),col("DrugStatus"),col("FullyApproved"))).as("PriorAuthorizationActivities"),
        collect_set(when(col("DiagnosisType") === "Secondary", struct(col("DiagnosisType"),col("DiagnosisCode")))).as("SecondaryDiagnosisDetails"),
        collect_set(when(col("DiagnosisType") === "Principal", struct(col("DiagnosisType"),col("DiagnosisCode")))).as("PrincipalDiagnosisDetails")

      )
      .saveToEs(s"$queryName/_doc")


  }

  def savePriorRequests(db: String): Unit = {
    val viewpipex = s"""
SELECT pr.SenderId AS PRSenderID,
pr.ReceiverId AS PRReceiverID,
pr.CreatedOn,
pa.SenderId AS PaSenderID,
pa.ReceiverId AS PAReceiverID,
pa.EndDate,

pra.ActivityCode,
pra.ActivityNetAmt,
pra.ActivityReference AS PRAActivityReference,
pra.ActivityType,

paa.ActivityCode AS PAActivityCode,
paa.ActivityListAmt AS PaaActivityListAmt,
paa.ActivityPaymentAmt,
paa.ActivityPatientShareAmt,
paa.ActivityReference AS PAAActivityReference,
paa.DenialCode,
CASE WHEN paa.ActivityPaymentAmt > 0 THEN 'Approved' ELSE 'Rejected' END AS DrugStatus,
CASE WHEN paa.ActivityNetAmt = paa.ActivityPaymentAmt + paa.ActivityPatientShareAmt THEN 'Yes' ELSE 'No' END AS FullyApproved,

prd.DiagnosisCode,
prd.DiagnosisType,

c.ClientName,

ct.ClientTypeName
FROM            dbo.PriorRequests AS pr WITH (nolock) INNER JOIN
                         dbo.PriorRequestsFiles AS prf WITH (nolock) ON pr.FileId = prf.FileId INNER JOIN
                         dbo.Clients AS c WITH (nolock) ON prf.ClientId = c.ClientId INNER JOIN
                         dbo.ClientTypes AS ct WITH (nolock) ON c.ClientTypeId = ct.ClientTypeId INNER JOIN
                         dbo.PriorRequestActivities AS pra WITH (nolock) ON pr.PriorRequestId = pra.PriorRequestId INNER JOIN
                         dbo.PriorRequestDiagnosis AS prd WITH (nolock) ON pr.PriorRequestId = prd.PriorRequestId LEFT OUTER JOIN
                         dbo.PriorAuthorizations AS pa WITH (nolock) ON pa.AuthorizationID = pr.AuthorizationID LEFT OUTER JOIN
                         dbo.PriorAuthorizationsFiles AS paf WITH (nolock) ON pa.FileId = paf.FileId LEFT OUTER JOIN
                         dbo.PriorAuthorizationActivities AS paa WITH (nolock) ON pa.PriorAuthorizationId = paa.PriorAuthorizationId AND pra.ActivityCode = paa.ActivityCode AND pra.ActivityReference = paa.ActivityReference



"""

    pipeline_prior_request("nano_prior_requests_data",viewpipex,db,"CreatedOn")
  }

  def pipeline(queryName:String,query:String,db:String,trackingColumnName:String) = {
    var data: DataFrame = null

    val lastpkOffset = readPKOffset(s"/offsets/$queryName")
    data = getdata(query,db).filter(col(trackingColumnName) > lastpkOffset)

    if(data.count > 0){
      val newOffset = data.orderBy(desc(trackingColumnName)).select(trackingColumnName).head.getAs[Integer](0)
      savePKOffset(s"/offsets/$queryName",newOffset)
    }

    data
  }

  def saveErxRequests(db: String): Unit = {
    val request = s"""SELECT
	   [RXId]
      ,erx.[SenderId]
      ,erx.[ReceiverId]
      ,erx.[DispositionFlag]
      ,[Type]
      ,[PayerID]
      ,[Clinician]
      ,ed.DiagnosisCode
      ,ed.DiagnosisType,
	  erx.TransactionDate,
	  erAuth.StartDate
	  ,authact.[Quantity]
      ,authact.[Net]
      ,authact.[List]
      ,authact.[PatientShare]
      ,authact.[PaymentAmount]
      ,erAuth.[DenialCode],
	  erAuth.Result,
	  authact.DenialCode as dc,
	  authact.eRxAuthorizationActivityId,
	  erAct.ActivityId,
	  erAct.ActivityCode
  FROM [Nano_eClaimsProviders].[dbo].[eRxRequests] erx
  join [Nano_eClaimsProviders].[dbo].eRXRequestsDiagnosis ed on erx.RXId = ed.eRxId
  join [Nano_eClaimsProviders].[dbo].[eRxRequestsActivities] erAct on erx.RXId=erAct.ErxId
  join [Nano_eClaimsProviders].[dbo].[eRxAuthorizations] erAuth on erx.ID= erAuth.AuthorizationID
  join Nano_eClaimsProviders.dbo.eRxAuthorizationActivities authact on erAuth.eRxAuthorizationId=authact.eRxAuthorizationId and erAct.ActivityReference = authact.ActivityReference

  """


    val icdcodemajor = spark.read.parquet("/profiles/ICDCodeMajorSubCategory").withColumn("PkeyDiagnosis",col("Pkey"))
    val icdcodes = spark.read.parquet("/profiles/ICD10CMCodes")
    val clinician = spark.read.parquet("/profiles/clinician")
      .withColumn("ClinicianCreatedDateTime",col("CreatedDateTime"))
      .withColumn("ClinicianLastEditDateTime",col("LastEditDateTime"))

    val denialReasons = spark.read.parquet("/profiles/denialReasons")
    val drugs = spark.read.parquet("/profiles/drugs")

    val payers = spark.read.parquet("/profiles/payers")
      .withColumn("PayerIdPkey",col("PayerId"))
      .drop(col("PayerId"))

    val facility = spark.read.parquet("/profiles/facility")
      .withColumn("FacilityCreatedDateTime",col("CreatedDateTime"))
      .withColumn("FacilityLastEditDateTime",col("LastEditDateTime"))
      .withColumn("PkeyFacility",col("Pkey"))

    val data = pipeline("erx_requests_data",request,db,"eRxAuthorizationActivityId")
      .join(clinician,col("Clinician") === col("ClinicianID"),"inner")
      .join(payers,col("PayerID") === col("PayerLicenceReference"))
      .join(facility,col("SenderId") === col("ProviderID"),"inner")
      .join(drugs, col("ActivityCode") === col("DrugCode") )
      .join(denialReasons, col("DenialCode") === col("DenialReasonCode") )
      .join(icdcodemajor,col("DiagnosisCode").between(col("RangeStart"),col("RangeEnd")))
      .withColumn("Quantity",col("Quantity").cast(DoubleType))
      .withColumn("Net",col("Net").cast(DoubleType))
      .withColumn("PatientShare",col("PatientShare").cast(DoubleType))
      .withColumn("PaymentAmount",col("PaymentAmount").cast(DoubleType))
      .withColumn("TransactionDate",date_format(col("TransactionDate"), "yyyy/MM/dd HH:mm:ss"))
      .groupBy(col("RXId"))
      .agg(
        first(struct(col("RXId"),col("TransactionDate"),col("DispositionFlag"),col("Type"))).as("PKEY"),
        first(struct(col("CliniD"),col("CliniName"),col("CliniGender"),col("CliniFacilityName"),col("CliniFacilityID"),col("CliniFacilityLocation"),col("CliniStatus"),col("CliniActiveFrom"),
          col("CliniActiveTo"),col("CliniMajorDept"),col("CliniMajorSpecial"),col("CliniCountry"),col("CliniLang"),col("ClinicianID"),col("ClinicianCreatedDateTime"),col("ClinicianLastEditDateTime"))).as("ClinicianDetails"),
        first(struct(col("PayerName"),col("PayerLicenceReference"),col("PayerLicneceSourceId"),col("PayerId"),col("PayerType"))).as("PayerDetails"),
        first(struct(col("PkeyFacility"),col("FacilityName"),col("FacilityLicenseTypeKey"),col("FacilityID"),col("FacilityPhone"),col("FacilityEmail"),col("FacilityCategory"),col("FacilitySpecialization"),col("FacilityRegion"),col("FacilityCreatedDateTime"),col("FacilityLastEditDateTime"))).as("ProviderDetails"),
        collect_set(when(col("DiagnosisType") === "Secondary", struct(col("PkeyDiagnosis"),col("DiagnosisCode"),col("DocSpeciality1"),col("DocSpeciality2"),col("DocSpeciality3"),col("ICDMajorSubCategory"),col("ICDCodeRange"),col("RangeStart"),col("RangeEnd"),col("DiagnosisType")))).as("SecondaryDiagnosisDetails"),
        collect_set(when(col("DiagnosisType") === "Principal", struct(col("PkeyDiagnosis"),col("DiagnosisCode"),col("DocSpeciality1"),col("DocSpeciality2"),col("DocSpeciality3"),col("ICDMajorSubCategory"),col("ICDCodeRange"),col("RangeStart"),col("RangeEnd"),col("DiagnosisType")))).as("PrincipalDiagnosisDetails"),
        collect_set(struct("ActivityId","DrugCode","TradeName","eRxAuthorizationActivityId","Quantity","Net","PatientShare","PaymentAmount","Result","DenialCode","DenialReasonDescription").as("ActivityDiagnosisDetails")).as("eRXActivitiesDetails")
      )

      .saveToEs("erx_requests_data/_doc", Map("es.mapping.id" -> "RXId"))
  }
}


object ETLEngine extends App {
  val spark = SparkSession.builder().getOrCreate()

  val executor = Executors.newSingleThreadScheduledExecutor()
  val cmd = parseArgs(args)

  val db = "jdbc:sqlserver://192.168.5.222;database=Nano_eClaimsProviders"
  val etl = new ETLEngine(spark)

  val staticRunnable = new Runnable {
    override def run(): Unit = etl.saveStaticData(db)
  }

  val masterRunnable = new Runnable {
    override def run(): Unit = etl.saveMastersData(db)
  }

  val priorRunnable = new Runnable{
    override def run(): Unit = etl.savePriorRequests(db)
  }

  val erxRunnable = new Runnable{
    override def run(): Unit = etl.saveErxRequests(db)
  }

  executor.scheduleAtFixedRate(
    staticRunnable,
    0,
    1,
    TimeUnit.DAYS
  )
  executor.scheduleAtFixedRate(
    masterRunnable,
    0,
    1,
    TimeUnit.DAYS
  )
  executor.scheduleAtFixedRate(
    priorRunnable,
    0,
    5,
    TimeUnit.MINUTES
  )
  executor.scheduleAtFixedRate(
     erxRunnable,
    0,
    5,
    TimeUnit.MINUTES
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