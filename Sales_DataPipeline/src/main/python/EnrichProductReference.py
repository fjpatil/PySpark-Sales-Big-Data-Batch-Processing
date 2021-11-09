from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType,DoubleType
import configparser
from src.main.python.gkfunctions import read_schema
from datetime import date,time,datetime,timedelta
from pyspark.sql import functions as F

# Initiating Spark Session
spark = SparkSession.builder.appName("EnrichProductReference").master("local").getOrCreate()

# Reading Configs
config = configparser.ConfigParser()
config.read(r"../projectconfigs/config.ini")
inputLocation = config.get("paths", "inputLocation")
inputSchemaFromConf = config.get("schema","inputSchema")
productPriceReferenceSchemaFromConf = config.get("schema","productPriceReferenceSchema")

outputLocation = config.get("paths", "outputLocation")

# read schema from config file
validFileSchema = read_schema(inputSchemaFromConf)
productPriceReferenceSchema = read_schema(productPriceReferenceSchemaFromConf)

currDayZoneSuffix = "_05062020"
prevDayZoneSuffix = "_04062020"

# Reading Valid data
validDataDF = spark.read\
    .schema(validFileSchema)\
    .option("delimiter", "|")\
    .option("header",True)\
    .csv(outputLocation+"Valid/ValidData"+currDayZoneSuffix)

validDataDF.createOrReplaceTempView("validDataDF")

# Reading Product Reference
productPriceReferenceDF = spark.read\
    .schema(productPriceReferenceSchema)\
    .option("delimiter", "|")\
    .option("header",True)\
    .csv(inputLocation+"Products")

productPriceReferenceDF.createOrReplaceTempView("productPriceReferenceDF")

productEnrichedDF = spark.sql("SELECT a.Sale_ID,a.Product_ID,b.Product_Name, "
                              "a.Quantity_Sold,a.Vendor_ID,a.Sale_Date, "
                              "b.Product_Price * a.Quantity_Sold as Sale_Amount, "
                              "a.Sale_Currency "
                              "from validDataDF a INNER JOIN productPriceReferenceDF b "
                              "ON a.Product_ID = b.Product_ID")

productEnrichedDF.write\
    .option("header",True)\
    .option("delimiter","|")\
    .mode("overwrite")\
    .csv(outputLocation+"Enriched\SaleAmountEnrichment\SaleAmountEnriched"+currDayZoneSuffix)
