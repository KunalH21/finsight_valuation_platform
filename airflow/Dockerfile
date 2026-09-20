
FROM apache/airflow:3.1.8

USER root

RUN apt-get update && apt-get install -y default-jdk
RUN ln -s /usr/bin/python3 /usr/bin/python


USER airflow


RUN pip install \
    pyspark==3.5.0 \
    yfinance \
    apache-airflow-providers-google