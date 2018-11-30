// Databricks notebook source
import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.functions.{ explode, split }
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Build connection string with the above information 
val connectionString = ConnectionStringBuilder("Endpoint=sb://iothub-ns-databricks-867376-4e04c9d40b.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=YBNaeQdmFt9/LY2O/yMVC/U4LbXhaa061t4unv+XOGE=;EntityPath=databricksdemoiothub")
  .setEventHubName("databricksdemoiothub")
  .build

val eventHubsConf = EventHubsConf(connectionString).setStartingPosition(EventPosition.fromEndOfStream);
//follow by the different options usable

val incomingStream = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .option("eventhubs.partition.count", "4")
  .load()

incomingStream.printSchema
//incomingStream.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

import org.apache.spark.sql.types._ // https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/package-summary.html
import org.apache.spark.sql.functions._

// Our JSON Schema
val jsonSchema = new StructType()
  .add("messageId", StringType)
  .add("deviceId", StringType)
  .add("temperature", StringType)
  .add("humidity", StringType)

// Convert our EventHub data, where the body contains our message and which we decode the JSON
val messages = incomingStream
  // Parse our columns from what EventHub gives us (which is the data we are sending, plus metadata such as offset, enqueueTime, ...)
  .withColumn("Offset", $"offset".cast(LongType))
  .withColumn("Time (readable)", $"enqueuedTime".cast(TimestampType))
  .withColumn("Timestamp", $"enqueuedTime".cast(LongType))
  .withColumn("Body", $"body".cast(StringType))
  // Select them so we can play with them
  .select("Offset", "Time (readable)", "Timestamp", "Body")
  // Parse the "Body" column as a JSON Schema which we defined above
  .select(from_json($"Body", jsonSchema) as "sensors")
  // Now select the values from our JSON Structure and cast them manually to avoid problems
  .select(
    $"sensors.messageId".cast("string"),
    $"sensors.deviceId".cast("string"), 
    $"sensors.temperature".cast("double") as "tempVal", 
    $"sensors.humidity".cast("double") as "humVal"
  )
messages.printSchema()


// COMMAND ----------

messages.createOrReplaceTempView("dataStreamsView")



// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM dataStreamsView

// COMMAND ----------

// MAGIC %sql
// MAGIC select avg(tempVal) as MeanTemparature,avg(humVal) as MeanHumidity  from dataStreamsView
