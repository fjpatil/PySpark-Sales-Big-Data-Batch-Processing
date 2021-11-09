# All Necessary Functions
from pyspark.sql.types import StructType
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType,DoubleType,FloatType

def read_schema(schema_arg):
    d_types = {
        "StringType()": StringType(),
        "IntegerType()": IntegerType(),
        "TimestampType()": TimestampType(),
        "DoubleType()": DoubleType(),
        "FloatType()": FloatType()
    }
    split_values = schema_arg.split(",")
    sch = StructType()
    for i in split_values:
        x = i.split(" ")

        sch.add(x[0],d_types[x[1]],True)

    return sch