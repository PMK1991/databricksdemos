// Databricks notebook source
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "26f0200e-0880-4d27-9691-0e6323c267bf")
spark.conf.set("dfs.adls.oauth2.credential", "FRzYOFb46pZh8Jf5+hWMw3l1pTJybf6kcQ/e197MTTo=")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

// COMMAND ----------

//spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
//spark.conf.set("dfs.adls.oauth2.client.id", "<Application ID>")
//spark.conf.set("dfs.adls.oauth2.credential", "<Key>")
//spark.conf.set("dfs.adls.oauth2.refresh.url", "<https://login.microsoftonline.com/<Directory ID>>")

// COMMAND ----------

val jdbcUsername = dbutils.secrets.get(scope = "key-vault-secrets", key = "username")
val jdbcPassword = dbutils.secrets.get(scope = "key-vault-secrets", key = "password")

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

val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("adl://srcdatalake.azuredatalakestore.net/FinancialData/cs-training_new.csv")

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


