# Install the required libraries
pip install "confluent-kafka[avro,json,protobuf]"

dbutils.library.restartPython()
from pyspark.sql.functions import expolode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.sql.functions import col
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import from_json
from pyspark.sql.functions import date_format
from pyspark.sql.functions import to_date
from datetime import datetime
import pyspark.sql.functions as fn
from pyspark.sql.avro.functions import from_avro

# Set up the variables in Databricks widgets
dbutils.widgets.text("kafka_topic_name", "", "kafka_topic_name")
dbutils.widgets.text("data_field_name", "", "data_field_name")
dbutils.widgets.text("chk_location", "", "chk_location")
dbutils.widgets.text("stg_location", "", "stg_location")
dbutils.widgets.text("raw_database", "", "raw_database")
dbutils.widgets.text("raw_table", "", "raw_table")
dbutils.widgets.text("unity_catalog_name", "", "unity_catalog_name")

stg_location = "/Volumes/your_folder_layer/rawlanding/" + kafka_topic_name
run_stg_location = stg_location + datetime.today().strftime('%Y%m%d_%H%M%S_%s')
kafka_bootstrap_servers = "your_kafka_bootstrap_servers"

# Define the configuration for the schema registry
from confluent_kafka import SchemaRegistryClient
schema_registry_conf = {
    'url': 'https://your_schema_registry_url',
    'ssl.ca.location': '/Volumes/your_folder_layer/ca.crt',
    'ssl.key.location': '/Volumes/your_folder_layer/key.pem',
    'ssl.certificate.location': '/Volumes/your_folder_layer/cert.pem',
}

# Create the schema registry client to talk to the schema registry
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Get the latest schema for the topic
keyRestResponseSchema  = schema_registry_client.get_latest_version(kafka_topic_name + "-key")
valueRestResponseSchema = schema_registry_client.get_latest_version(kafka_topic_name + "-value")
confluentKeySchema = keyRestResponseSchema.schema_str
confluentValueSchema = valueRestResponseSchema.schema_str

# Get the key/value trailing bytes of the message, which equal to the length of the schema id
keytrailingbytes = len(str(schema_registry_client.get_latest_version(kafka_topic_name + "-key").schema_id))
valuetrailingbytes = len(str(schema_registry_client.get_latest_version(kafka_topic_name + "-value").schema_id))

# Set the option for how the process fail - either stop at the first failure it finds (FAILFAST) or set corrupt data to null (PERMISSIVE)
fromAvroOptions = {"mode": "PERMISSIVE"}

# Read data from Kafka topic from the earliest offset
kafka_df = (

    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic_name)
    .option("startingOffsets", "earliest")
    .option("security.protocal", "SSL")
    .option("kafka.ssl.enabled.protocols", "TLSv1.2")
    .option("kafka.ssl.truststore.location", "/Volumes/your_folder_layer/kafka.truststore.jks")
    .option("kafka.ssl.truststore.password", "your_password")
    .option("kafka.ssl.keystore.location", "/Volumes/your_folder_layer/kafka.keystore.jks")
    .option("kafka.ssl.keystore.password", "your_password")
    .option("kafka.ssl.key.password", "your_password")
    .option("kafka.ssl.trustore.type", "JKS")
    .option("kafka.keystore.type", "JKS")
    .option("failOnDataLoss", "false")
    .load()
    .withColumn("fixed_key", fn.expr("substring(key, 6, length(key) - {})".format(keytrailingbytes)))
    .withColumn("fixed_value", fn.expr("substring(value, 6, length(value) - {})".format(valuetrailingbytes)))
    .select('partition', 'offset', from_avro()('fixed_key', confluentKeySchema, fromAvroOptions).alias('parsedkey'), from_avro('fixed_value', confluentValueSchema, fromAvroOptions).alias('parsedvalue')
)

# Flatten the DataFrame
df_flattened = kafka_df.select("*", "parsedValue." + data_field_name + ".*", "parsedValue.headers.*")

# Write the Stream to the specified location
(
    df_flattened.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(availableNow=True)
    .option("checkpointLocation", chk_location)
    .option("path", run_stg_location)
    .start()
    .awaitTermination(30)
)

# Do some transformations on the DataFrame
stg_df = spark.read.format("delta".load(run_stg_location))
final_df = stg_df.select("*", to_date(stg)df.timestamp, "yyyy-MM-dd'T'HH:mm:ss/SSS").alias("load_timestamp")
final_df.write.partitionBy("load_timestamp").format("delta").mode("append").saveAsTable(unity_catalog_name + "." + raw_database + "." + raw_table)