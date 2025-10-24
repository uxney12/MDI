FROM apache/spark:3.5.0

USER root
RUN pip install --no-cache-dir pyspark
RUN pip install --no-cache-dir pyarrow boto3

RUN mkdir -p /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar -P /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P /opt/spark/jars

WORKDIR /app
COPY job.py /app/
CMD ["/opt/spark/bin/spark-submit", "--jars", "/opt/spark/jars/*", "job.py"]
