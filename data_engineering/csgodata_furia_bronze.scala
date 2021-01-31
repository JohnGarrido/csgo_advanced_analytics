// Databricks notebook source
// Setting up AS3

val bucketName = "csgodatas3"
val mountName = "s3data"

val crd = spark.read.option("header","true").csv("/FileStore/tables/new_user_credentials.csv")
  .select("*")

val accessKey = crd.select("Access key ID")
  .collect()(0)(0);

val secretKey = crd.select("Secret access key")
  .collect()(0)(0);

val exportPath = "s3n://"+accessKey+":"+secretKey+"@"+bucketName
val mountPath = "/mnt/"+mountName

// COMMAND ----------

/// dbutils.fs.mount(exportPath, mountPath)


// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val inboundFile = "/FileStore/tables/json_raw/"

val bronzeDF = spark.read.option("multiline",true).json(inboundFile)

// COMMAND ----------

val dbName = "csgodata_furia_bronze"
val tbName = "ids"
val tableId = s"$dbName.$tbName"

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE DATABASE IF NOT EXISTS csgodata_furia_bronze

// COMMAND ----------

if(!spark.catalog.tableExists(tableId)) {
  bronzeDF.write
       .format("delta")
       .mode("append")
       .option("path", mountPath+"/pipelineExport/bronze/")
       .saveAsTable(tableId)
  
} else {
 bronzeDF.createOrReplaceTempView("vw_source")
 spark.sql(s"""
   MERGE INTO ${tableId} as target
   USING vw_source as source
   ON target.id = source.id
   WHEN NOT MATCHED THEN
     INSERT *
 """)
}

// last value ref = 21

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(DISTINCT id)
// MAGIC FROM csgodata_furia_bronze.ids

// COMMAND ----------


