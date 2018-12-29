// Databricks notebook source
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "817e80ee-4aae-41fb-b88d-ca588ed60e4f")
spark.conf.set("dfs.adls.oauth2.credential", "wmts8PmtqHUbyWJMEuQOAsWANFWpcIfMOI1UDOQ7ZZs=")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

// COMMAND ----------

//spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
//spark.conf.set("dfs.adls.oauth2.client.id", "<Application ID>")
//spark.conf.set("dfs.adls.oauth2.credential", "<Key>")
//spark.conf.set("dfs.adls.oauth2.refresh.url", "<https://login.microsoftonline.com/<Directory ID>/oauth2/token>")

// COMMAND ----------

val jdbcUsername = dbutils.secrets.get(scope = "AvroScope", key = "username")
val jdbcPassword = dbutils.secrets.get(scope = "AvroScope", key = "password")

// COMMAND ----------

val jdbcHostname = "newsignature.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "NewSignature"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")

// COMMAND ----------

val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("adl://srcdl.azuredatalakestore.net/FinancialData/cs-training_new.csv")

// COMMAND ----------

val df = spark.read.csv("adl://srcdl.azuredatalakestore.net/FinancialData/cs-training_new.csv")

// COMMAND ----------

df.show()


// COMMAND ----------


val df1=df.filter("DebtRatio<1")
val df2=df1.filter("MonthlyIncome <> 'NA'")




// COMMAND ----------

import org.apache.spark.sql.functions._
val finaldf =df2.withColumn("IncomeConsumption", df2("MonthlyIncome")/(df2("NumberOfDependents")+1))


// COMMAND ----------


finaldf.write.mode("Overwrite").jdbc(jdbcUrl, "creditdata", connectionProperties)


