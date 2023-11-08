// Databricks notebook source
// MAGIC %md
// MAGIC #Scala and Spark basics Capstone Project: Advanced JSON Manipulation using Spark on Databricks

// COMMAND ----------

// MAGIC %md
// MAGIC ## Phase 01: Generating a Comprehensive Fake JSON Database

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ###Store the Json on DBFS And Load the hefty JSON

// COMMAND ----------

val pathToJson = "/FileStore/tables/generated.json"
val rawDf = spark.read.option("multiline","true").json(pathToJson)

// COMMAND ----------

//Storing the data based on medallion architecture 
rawDf.write
  .format("parquet")
  .mode("overwrite")
  .save("/FileStore/tables/bronze/raw_users")

// COMMAND ----------

val flattenedDF = rawDf
  // Explode arrays
  .withColumn("address", explode($"address"))
  .withColumn("employment", explode($"employment"))
  .withColumn("tags", concat_ws(",", $"tags"))
  
  // Flatten nested fields
  .withColumn("street", $"address.street")
  .withColumn("city", $"address.city")
  .withColumn("state", $"address.state")
  .withColumn("zipcode", $"address.zipcode")
  .withColumn("country", $"address.country")
  .withColumn("job_title", $"employment.job_title")
  .withColumn("department", $"employment.department")
  .withColumn("company", $"employment.company")

  // drop of original nested columns
  .drop("address", "employment")

  flattenedDF.show(false)

// COMMAND ----------

flattenedDF.createOrReplaceTempView("users_temp")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Phase 02: Deft JSON Data Handling in Scala

// COMMAND ----------

// MAGIC %md
// MAGIC ### i. Filtering
// MAGIC
// MAGIC
// MAGIC **- Cull records for users who joined after a particular date.**
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC 1 - SQL query to obtain the data that we need.

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from users_temp where joined_date > '2020-08-08';

// COMMAND ----------

// MAGIC %md
// MAGIC 2 - Using spark to obtain the data that we need

// COMMAND ----------

val filterDate: String = "2020-08-08"
val dfWithFilteredDate = flattenedDF.filter(f"joined_date > '${filterDate}'")

dfWithFilteredDate.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC **- Isolate users based on specific tags, e.g., "developer".**

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC 1 - SQL query to obtain the data that we need.

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select * 
// MAGIC from users_temp 
// MAGIC where CONTAINS( tags, 'qui' );
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC 2 - Using spark to obtain the data that we need

// COMMAND ----------

val tag: String = "qui"
val dfWithSpecificTag = flattenedDF.filter(array_contains(split(col("tags"), ","), tag))

dfWithSpecificTag.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC - **Pick out users based on their active status.**

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC 1 - SQL query to obtain the data that we need.

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select * 
// MAGIC from users_temp
// MAGIC where is_active = true;

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC 2 - Using spark to obtain the data that we need

// COMMAND ----------

val dfUsersActive = flattenedDF.filter("is_active = 'true'")
dfUsersActive.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ###ii. Data Flattening
// MAGIC **- De-nest the address and employment structures to a flat structure..**

// COMMAND ----------

/* Done before for temp table purpose creation, this was the code for it:

val flattenedDF = rawDf
  // Explode arrays
  .withColumn("address", explode($"address"))
  .withColumn("employment", explode($"employment"))
  .withColumn("tags", concat_ws(",", $"tags"))
  
  // Flatten nested fields
  .withColumn("street", $"address.street")
  .withColumn("city", $"address.city")
  .withColumn("state", $"address.state")
  .withColumn("zipcode", $"address.zipcode")
  .withColumn("country", $"address.country")
  .withColumn("job_title", $"employment.job_title")
  .withColumn("department", $"employment.department")
  .withColumn("company", $"employment.company")

  // drop of original nested columns
  .drop("address", "employment")

  */

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ###iii. Data Enrichment
// MAGIC
// MAGIC **- Craft a location field by melding city, state, and country.**

// COMMAND ----------

val enrichedDfWithLocation = flattenedDF.withColumn("location", concat_ws(", ", $"city", $"state", $"country"))

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC **-Compute and introduce a phoneCount field delineating the count of phone numbers each user possesses.**

// COMMAND ----------

// We have only 1 phone per line so for now will just populate it with 1

val enrichedDfWithPhoneCount = enrichedDfWithLocation.withColumn("phoneCount", lit(1))

// COMMAND ----------

//Storing the data based on medallion architecture 
enrichedDfWithPhoneCount.write
  .format("parquet")
  .mode("overwrite")
  .save("/FileStore/tables/silver/enriched_users")

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ###iv. Textual Alterations
// MAGIC
// MAGIC **- Transform email addresses into lowercase.**

// COMMAND ----------

val dfWithLowercaseEmail = enrichedDfWithPhoneCount.withColumn("email", lower($"email"))

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC **-Craft a nameInitials field, representing the initials of user names.**

// COMMAND ----------

val dfWithInitials = dfWithLowercaseEmail.withColumn(
  "nameInitials",
  concat(
    upper(substring(split($"name", " ")(0), 1, 1)), // First initial
    upper(substring(split($"name", " ")(1), 1, 1))  // Last initial
  )
)

dfWithInitials.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ### v. Array-based Tasks
// MAGIC
// MAGIC **-Spotlight users boasting more than one phone number.**

// COMMAND ----------

val dfWithHigherThanOnePhone = dfWithInitials.filter($"phoneCount" >= 1)

dfWithHigherThanOnePhone.foreach(row => println(s"${row.getAs[String]("name")} has more than one phone number!"))

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC **-Morph the array of tags into a solitary comma-separated string.**

// COMMAND ----------

/* it is already done, but it is done in this way:

val flattenTag = dfWithInitials.withColumn("tags", concat_ws(",", $"tags"))

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ### Output
// MAGIC
// MAGIC **- Warehouse the manipulated data as a CSV file with columns such as id, name, email, street, city, zipcode, phoneNumbers, and any novel columns you've injected.**

// COMMAND ----------

dfWithInitials.write
  .option("header", "true")
  .csv("/FileStore/enriched/enriched_data.csv")
