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

'''
KEY_MAPPING = {
    "Total Revenue": "revenue",
    "Reconciled Depreciation": "depr",
    "Net Income": "net_income",
    "Operating Expenses": "operating_expenses",
    "Gross Profit": "gross_profit",
    "Operating Income": "operating_income",
    "Interest Expense": "interest_expense",
    "Tax Provision": "tax_provision",
    "Operating Cash Flow": "operating_cash_flow",
    "Capital Expenditures": "capex",
    "Cash and Cash Equivalents": "cash_and_equivalents",
    "Total Assets": "total_assets",
    "Total Liabilities": "total_liabilities",
}

'''