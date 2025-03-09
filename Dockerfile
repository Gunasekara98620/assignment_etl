# Using Debian base image instead of openjdk for better flexibility
FROM debian:bookworm-slim

# Set environment variables for Spark, Hadoop, and Java
ENV SPARK_VERSION=3.5.5 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PYSPARK_HOME=/opt/spark/python \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYSPARK_HOME \
    PATH=$PATH:/opt/spark/bin:/opt/spark/sbin

# Install dependencies for Conda, Spark, Hadoop, and other utilities
RUN apt-get update && \
    apt-get install -y software-properties-common gnupg2 wget curl bash tini lsb-release \
    libsnappy-dev openjdk-17-jdk python3 python3-pip python3-distutils python3-venv curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Conda (Miniconda is a lightweight version of Anaconda)
RUN wget -q https://repo.anaconda.com/miniconda/Miniconda3-py39_4.12.0-Linux-x86_64.sh -O miniconda.sh && \
    bash miniconda.sh -b -p /opt/conda && \
    rm miniconda.sh

# Set Conda paths
ENV PATH=/opt/conda/bin:$PATH

# Install Python 3.11
RUN conda install python=3.11 -y

# Download and install Apache Spark with Hadoop
RUN wget -qO - https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar xvz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# Install Python dependencies using Conda
RUN conda install -y \
    pytorch \
    pyspark \
    s3fs

# Install any other pip dependencies (if required)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set the working directory
WORKDIR /app

# Copy the application files to the container
COPY src/ src/
COPY script/ script/
COPY config/ config/

## Make the run.sh script executable
#RUN chmod +x script/run.sh
#
## Expose Spark UI port
#EXPOSE 4040
#
## Set the entrypoint using Tini for proper signal handling
#ENTRYPOINT ["tini", "--", "script/run.sh"]
