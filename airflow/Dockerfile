# 1. Start with the official Airflow image
FROM apache/airflow:3.1.8

# 2. Switch to 'root' (Administrator) to install system tools
USER root

# 3. Install Java (Spark requires Java to run)
RUN apt-get update && apt-get install -y default-jdk
RUN ln -s /usr/bin/python3 /usr/bin/python
# 4. Switch back to the 'airflow' user for safety
USER airflow

# 5. Pre-install the big libraries so we don't download them at startup
RUN pip install \
    pyspark==3.5.0 \
    yfinance \
    dbt-core \
    dbt-snowflake