FROM python:3.11-slim

ARG openjdk_version="17"

# USER root 

RUN apt-get update --yes && \
    apt-get install --yes  \
        curl \
        "openjdk-${openjdk_version}-jre-headless" ca-certificates-java procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


RUN curl -O -L https://dlcdn.apache.org/spark/spark-3.4.3/spark-3.4.3-bin-hadoop3.tgz \
    && tar -zxvf spark-3.4.3-bin-hadoop3.tgz \
    && rm -rf spark-3.4.3-bin-hadoop3.tgz \
    && mv spark-3.4.3-bin-hadoop3/ /usr/local/ \
    && rm -rf /usr/local/spark \
    && rm -rf /usr/local/spark-3.3.0-bin-hadoop3 \
    && ln -s /usr/local/spark-3.4.3-bin-hadoop3 /usr/local/spark


RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
#    && mkdir -p /usr/local/spark/jars \
    && mv s3-2.18.41.jar /usr/local/spark/jars \
    && mv aws-java-sdk-1.12.367.jar /usr/local/spark/jars \
    && mv aws-java-sdk-bundle-1.11.1026.jar /usr/local/spark/jars \
    && mv delta-core_2.12-2.2.0.jar /usr/local/spark/jars \
    && mv delta-storage-2.2.0.jar /usr/local/spark/jars \
    && mv hadoop-aws-3.3.2.jar /usr/local/spark/jars


WORKDIR /opt/dagster/app
COPY requirements.txt /opt/dagster/app
RUN pip install --upgrade pip && pip install -r requirements.txt
COPY . /opt/dagster/app

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline"]