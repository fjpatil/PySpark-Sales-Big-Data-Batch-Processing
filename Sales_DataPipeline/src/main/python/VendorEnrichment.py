from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType,DoubleType
import configparser
from src.main.python.gkfunctions import read_schema
from datetime import date,time,datetime,timedelta
from pyspark.sql import functions as F

# Initiating Spark Session
spark = SparkSession.builder.appName("VendorEnrichment").master("local").getOrCreate()

# Reading Configs
config = configparser.ConfigParser()
config.read(r"../projectconfigs/config.ini")
inputLocation = config.get("paths", "inputLocation")
inputSchemaFromConf = config.get("schema","inputSchema")
productPriceReferenceSchemaFromConf = config.get("schema","productPriceReferenceSchema")
vendorReferenceSchemaFromConf = config.get("schema","vendorReferenceSchema")
productEnrichedReferenceSchemaFromConf = config.get("schema","productEnrichedReferenceSchema")
usdReferenceSchemaFromConf = config.get("schema","usdReferenceSchema")

outputLocation = config.get("paths", "outputLocation")

# read schema from config file
validFileSchema = read_schema(inputSchemaFromConf)
productPriceReferenceSchema = read_schema(productPriceReferenceSchemaFromConf)
vendorReferenceSchema = read_schema(vendorReferenceSchemaFromConf)
productEnrichedReferenceSchema = read_schema(productEnrichedReferenceSchemaFromConf)
usdReferenceSchema = read_schema(usdReferenceSchemaFromConf)

currDayZoneSuffix = "_05062020"
prevDayZoneSuffix = "_04062020"

productEnrichedDF = spark.read\
    .schema(productEnrichedReferenceSchema)\
    .option("header", True)\
    .option("delimiter", "|")\
    .csv(outputLocation+"Enriched\SaleAmountEnrichment\SaleAmountEnriched"+currDayZoneSuffix)

productEnrichedDF.createOrReplaceTempView("productEnrichedDF")

usdReferenceDF = spark.read\
    .schema(usdReferenceSchema) \
    .option("delimiter", "|") \
    .csv(inputLocation+"USD_Rates")

usdReferenceDF.createOrReplaceTempView("usdReferenceDF")

vendorReferenceDF = spark.read\
    .schema(vendorReferenceSchema)\
    .option("header",False)\
    .option("delimiter","|")\
    .csv(inputLocation+"Vendors")

vendorReferenceDF.createOrReplaceTempView("vendorReferenceDF")

vendorEnrichedDF = spark.sql("SELECT a.*,b.Vendor_Name "
                             "from productEnrichedDF a INNER JOIN vendorReferenceDF b "
                             "ON a.Vendor_ID = b.Vendor_ID ")

vendorEnrichedDF.createOrReplaceTempView("vendorEnrichedDF")

usdEnrichedDF = spark.sql("SELECT a.*, ROUND((a.Sale_amount/b.Exchange_Rate),2) as Amount_USD "
                          "from vendorEnrichedDF a JOIN usdReferenceDF b "
                          "ON a.Sale_Currency = b.Currency_Code ")

usdEnrichedDF.write\
    .option("delimiter","|")\
    .option("header",True)\
    .mode("overwrite")\
    .csv(outputLocation+"Enriched/Vendor_USD_Enriched\Vendor_USD_Enriched"+currDayZoneSuffix)

usdEnrichedDF.write.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/gkstorespipelinedb",
    driver = "com.mysql.jdbc.Driver",
    dbtable = "finalsales",
    user = "root",
    password = "admin").mode("append").save()