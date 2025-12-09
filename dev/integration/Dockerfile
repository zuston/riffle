FROM docker.io/openanolis/anolisos:8

RUN yum install -y libzip unzip wget cmake openssl-devel llvm clang-devel clang krb5-workstation git gcc gcc-c++

# install rust nightly toolchain
RUN curl https://sh.rustup.rs > /rustup-init
RUN chmod +x /rustup-init
RUN /rustup-init -y --default-toolchain nightly-2025-06-01-x86_64-unknown-linux-gnu

# install java
RUN yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel
RUN echo 'export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk"' >> ~/.bashrc

# install protoc
RUN wget -O /protobuf-21.7-linux-x86_64.zip https://github.com/protocolbuffers/protobuf/releases/download/v21.7/protoc-21.7-linux-x86_64.zip
RUN mkdir /protobuf-bin && (cd /protobuf-bin && unzip /protobuf-21.7-linux-x86_64.zip)
RUN echo 'export PATH="$PATH:/protobuf-bin/bin"' >> ~/.bashrc

# attach libjvm.so
RUN echo 'export LD_LIBRARY_PATH=${JAVA_HOME}/jre/lib/amd64/server:${LD_LIBRARY_PATH}' >> ~/.bashrc

# setup hadoop env
RUN curl -LsSf https://dlcdn.apache.org/hadoop/common/hadoop-3.3.5/hadoop-3.3.5.tar.gz | tar zxf - -C /root
RUN echo "export HADOOP_HOME=/root/hadoop-3.3.5" >> ~/.bashrc
RUN echo "export CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath --glob)" >> ~/.bashrc
RUN echo "export HDRS_NAMENODE=default" >> ~/.bashrc

# install python and pip
RUN yum install -y python3 python3-pip
RUN pip3 install --upgrade pip

# install netcat for port checking
RUN yum install -y nc

# install spark
ARG SPARK_VERSION=3.5.0
RUN cd /opt && \
    wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# install uniffle coordinator (must be before configuring it)
ARG UNIFFLE_VERSION=0.10.0
RUN cd /opt && \
    wget -q https://archive.apache.org/dist/uniffle/${UNIFFLE_VERSION}/apache-uniffle-${UNIFFLE_VERSION}-bin.tar.gz && \
    tar zxvf apache-uniffle-${UNIFFLE_VERSION}-bin.tar.gz && mv apache-uniffle-${UNIFFLE_VERSION}-hadoop2.8 uniffle && \
    rm -f apache-uniffle-${UNIFFLE_VERSION}-bin.tar.gz
ENV UNIFFLE_HOME=/opt/uniffle
ENV PATH="${UNIFFLE_HOME}/bin:${PATH}"

# configure uniffle coordinator (after UNIFFLE_HOME is set and uniffle is installed)
RUN mkdir -p ${UNIFFLE_HOME}/conf
COPY coordinator.conf ${UNIFFLE_HOME}/conf/coordinator.conf

# download uniffle spark client
RUN mkdir -p ${SPARK_HOME}/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/uniffle/rss-client-spark3-shaded/${UNIFFLE_VERSION}/rss-client-spark3-shaded-${UNIFFLE_VERSION}.jar \
    -O ${SPARK_HOME}/jars/rss-client.jar || \
    echo "Failed to download Uniffle Spark Client"

# setup riffle home directory
ENV RIFFLE_HOME=/opt/riffle
RUN mkdir -p ${RIFFLE_HOME}/conf

# configure riffle servers (place configs in RIFFLE_HOME/conf)
COPY riffle.conf.1 ${RIFFLE_HOME}/conf/riffle.conf.1
COPY riffle.conf.2 ${RIFFLE_HOME}/conf/riffle.conf.2

# configure spark to use riffle
COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

# setup environment variables
RUN echo "export SPARK_HOME=${SPARK_HOME}" >> ~/.bashrc && \
    echo "export UNIFFLE_HOME=${UNIFFLE_HOME}" >> ~/.bashrc && \
    echo "export RIFFLE_HOME=${RIFFLE_HOME}" >> ~/.bashrc && \
    echo "export PYTHONPATH=\${SPARK_HOME}/python:\${SPARK_HOME}/python/lib/py4j-*.zip:\${PYTHONPATH}" >> ~/.bashrc

RUN echo "export RUST_BACKTRACE=1" >> ~/.bashrc

ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH
RUN echo "$HADOOP_HOME/lib/native" >> /etc/ld.so.conf.d/hadoop.conf && ldconfig

# copy kyuubi tpcds gen jar into the SPARK_HOME jars
RUN cd ${SPARK_HOME}/jars && \
    wget https://repo1.maven.org/maven2/org/apache/kyuubi/kyuubi-spark-connector-tpcds_2.12/1.10.2/kyuubi-spark-connector-tpcds_2.12-1.10.2.jar

# copy sql_set files into the /tmp/sql_set directory
COPY sql_set /tmp/sql_set

# copy endpoint script
COPY endpoint.sh /usr/local/bin/endpoint.sh
RUN chmod +x /usr/local/bin/endpoint.sh
ENTRYPOINT ["/usr/local/bin/endpoint.sh"]
