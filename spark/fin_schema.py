from pyspark.sql.types import StructType, StructField, StringType, MapType, DoubleType

# We define the shape of a financial statement once
statement_map = MapType(StringType(), MapType(StringType(), DoubleType()))

income_statement_schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("income_statement", statement_map, True),
    StructField("balance_sheet", statement_map, True),
    StructField("cash_flow", statement_map, True)
])
