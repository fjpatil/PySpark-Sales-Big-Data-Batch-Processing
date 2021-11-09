from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType,DoubleType
import configparser
from src.main.python.gkfunctions import read_schema
from datetime import date,time,datetime,timedelta
from pyspark.sql import functions as F
# Initiating Spark Session
spark = SparkSession.builder.appName("DataInjestAndRefine").master("local").getOrCreate()

# Reading Configs
config = configparser.ConfigParser()
config.read(r"../projectconfigs/config.ini")
inputLocation = config.get("paths", "inputLocation")
inputSchemaFromConf = config.get("schema","inputSchema")
holdFileSchemaFromConf = config.get("schema","holdFileSchema")

outputLocation = config.get("paths", "outputLocation")

# read schema from config file
landingFileSchema = read_schema(inputSchemaFromConf)
holdFileSchema = read_schema(holdFileSchemaFromConf)


# Landing file schema creation
# landingFileSchema = StructType([
#     StructField("Sale_ID", StringType(), True),
#     StructField("Product_ID", StringType(), True),
#     StructField("Quantity_Sold", IntegerType(), True),
#     StructField("Vendor_ID", StringType(), True),
#     StructField("Sale_Date", TimestampType(), True),
#     StructField("Sale_Amount", DoubleType(), True),
#     StructField("Sale_Currency", StringType(), True)
# ])

# landingFileDataFrame = spark.read.format("csv")\
#     .schema(landingFileSchema)\
#     .option("delimiter", "|")\
#     .csv(inputLocation+"Sales_Landing\SalesDump_04062020")

# Handling dates
#today = datetime.now()
#yesterday = today - timedelta(1)

#current_day_suffix = "_"+today.strftime("%d%m%Y")
#previous_day_suffix = "_"+yesterday.strftime("%d%m%Y")

# print(previous_day_suffix)
# print(current_day_suffix)


currDayZoneSuffix = "_05062020"
prevDayZoneSuffix = "_04062020"

# Reading input data
landingFileDF = spark.read\
    .schema(landingFileSchema)\
    .option("delimiter", "|")\
    .csv(inputLocation + "Sales_Landing\SalesDump"+currDayZoneSuffix)

# landingFileDF.show()
#
# invalidDF = landingFileDF.filter(F.col("Quantity_Sold").isNull() | F.col("Vendor_ID").isNull())
# validDF = landingFileDF.filter(F.col("Quantity_Sold").isNotNull() & F.col("Vendor_ID").isNotNull())
# #
# # invalidDF.show()
# # validDF.show()
# #
# validDF.write\
#     .mode("overwrite")\
#     .option("delimiter", "|")\
#     .option("header",True)\
#     .csv(outputLocation+"Valid/ValidData"+prevDayZoneSuffix)
#
# invalidDF.write\
#     .mode("overwrite")\
#     .option("delimiter", "|")\
#     .option("header",True)\
#     .csv(outputLocation+"Hold/HoldData"+prevDayZoneSuffix)

#################################################################
landingFileDF.createOrReplaceTempView("landingFileDF")
#
previousHoldDf = spark.read\
    .schema(holdFileSchema)\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(outputLocation+"Hold/HoldData"+prevDayZoneSuffix)
#
previousHoldDf.createOrReplaceTempView("previousHoldDf")
#
refreshedLandingData = spark.sql("SELECT a.Sale_ID,a.Product_ID, "
                                 "CASE "
                                 "WHEN (a.Quantity_Sold IS NULL) THEN b.Quantity_Sold "
                                 "ELSE a.Quantity_Sold "
                                 "END AS Quantity_Sold, "
                                 "CASE "
                                 "WHEN (a.Vendor_ID IS NULL) THEN b.Vendor_ID "
                                 "ELSE a.Vendor_ID "
                                 "END AS Vendor_ID, "
                                 "a.Sale_Date,a.Sale_Amount,a.Sale_Currency "
                                 "from landingFileDF a left outer join previousHoldDf b "
                                 "ON a.Sale_ID = b.Sale_ID")

#refreshedLandingData.show()
###################################################################
validLandingRefreshDF = refreshedLandingData\
    .filter(F.col("Quantity_Sold").isNotNull() & F.col("Vendor_ID").isNotNull())

validLandingRefreshDF.createOrReplaceTempView("validLandingRefreshDF")

releaseFromHold = spark.sql("SELECT vd.Sale_ID "
                            "FROM validLandingRefreshDF vd INNER JOIN previousHoldDf phd "
                            "ON vd.Sale_ID = phd.Sale_ID")

releaseFromHold.createOrReplaceTempView("releaseFromHold")

notReleaseFromHold = spark.sql("SELECT * FROM previousHoldDf "
                                "WHERE Sale_ID NOT IN (SELECT Sale_ID FROM releaseFromHold)")

notReleaseFromHold.createOrReplaceTempView("notReleaseFromHold")

invalidLandingRefreshDF = refreshedLandingData\
    .filter(F.col("Quantity_Sold").isNull() | F.col("Vendor_ID").isNull())\
    .withColumn("Hold_Reason",F
                .when(F.col("Quantity_Sold").isNull(), "Quantity Sold is missing")
                .otherwise(F.when(F.col("Vendor_ID").isNull(),"Vendor ID is missing")))\
    .union(notReleaseFromHold)

validLandingRefreshDF.write\
    .mode("overwrite")\
    .option("delimiter", "|")\
    .option("header",True)\
    .csv(outputLocation+"Valid/ValidData"+currDayZoneSuffix)

invalidLandingRefreshDF.write\
    .mode("overwrite")\
    .option("delimiter", "|")\
    .option("header",True)\
    .csv(outputLocation+"Hold/HoldData"+currDayZoneSuffix)